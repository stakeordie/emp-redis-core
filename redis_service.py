#!/usr/bin/env python3
# Core Redis service for the queue system
import os
import json
import time
import redis
import redis.asyncio as aioredis
from typing import Dict, Any, Optional, List, Union, Callable
from .utils.logger import logger
from .interfaces import RedisServiceInterface

# Redis configuration
# When running in Docker, "REDIS_HOST" env var should be set to the container name (e.g., "hub")
# For local development, default to "localhost"
REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))
REDIS_DB = int(os.environ.get("REDIS_DB", 0))
REDIS_PASSWORD = os.environ.get("REDIS_PASSWORD", None)

# Redis host configuration

# Queue names
STANDARD_QUEUE = "job_queue"
PRIORITY_QUEUE = "priority_queue"

# Redis key prefixes
JOB_PREFIX = "job:"
WORKER_PREFIX = "worker:"

class RedisService(RedisServiceInterface):
    """Core service for interacting with Redis for job queue operations
    
    Implements the RedisServiceInterface to ensure consistent API across the application.
    """
    
    def __init__(self):
        """Initialize Redis connections"""
        # Synchronous client for most operations
        self.client = redis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            db=REDIS_DB,
            password=REDIS_PASSWORD,
            decode_responses=True,
        )
        
        # Asynchronous client for pub/sub
        self.async_client = None
        self.pubsub = None
        

    
    async def connect_async(self) -> None:
        """
        Connect to Redis asynchronously for pub/sub operations.
        
        This method establishes an asynchronous connection to Redis
        for operations that require pub/sub functionality.
        """
        if self.async_client is None:
            self.async_client = aioredis.Redis(
                host=REDIS_HOST,
                port=REDIS_PORT,
                db=REDIS_DB,
                password=REDIS_PASSWORD,
                decode_responses=True,
            )
            self.pubsub = self.async_client.pubsub()

    
    async def close_async(self) -> None:
        """
        Close the async Redis connection.
        
        This method properly closes the asynchronous Redis connection
        and cleans up any resources.
        """
        if self.pubsub:
            await self.pubsub.close()
        if self.async_client:
            await self.async_client.close()

    
    async def init_redis(self) -> bool:
        """
        Initialize Redis connections and data structures.
        
        This method ensures that both synchronous and asynchronous
        Redis connections are properly established and initializes
        any necessary data structures for the job queue system.
        
        Returns:
            bool: True if initialization was successful, False otherwise
        """
        # Ensure async connection is established
        await self.connect_async()
        
        # Initialize Redis data structures if they don't exist
        # Clear any stale temporary keys
        self.client.delete("temp:workers")
        
        # Create worker set if it doesn't exist
        if not self.client.exists("workers:all"):
            self.client.sadd("workers:all", "placeholder")  # Create the set
            self.client.srem("workers:all", "placeholder")  # Remove placeholder
            
        # Create idle workers set if it doesn't exist
        if not self.client.exists("workers:idle"):
            self.client.sadd("workers:idle", "placeholder")  # Create the set
            self.client.srem("workers:idle", "placeholder")  # Remove placeholder
            
        # Ensure queue structures exist
        if not self.client.exists(STANDARD_QUEUE):
            # Initialize empty list for standard queue
            self.client.rpush(STANDARD_QUEUE, "placeholder")  # Create the list
            self.client.lpop(STANDARD_QUEUE)  # Remove placeholder
            
        # Initialize job statistics counters if they don't exist
        stats_keys = ["stats:jobs:completed", "stats:jobs:failed", "stats:jobs:total"]
        for key in stats_keys:
            if not self.client.exists(key):
                self.client.set(key, 0)
                

        return True
    
    async def close(self) -> None:
        """
        Close all Redis connections.
        
        This method ensures that both synchronous and asynchronous
        Redis connections are properly closed and resources are released.
        """
        # Close async connections
        await self.close_async()
        
        # Close synchronous connection
        if self.client:
            self.client.close()

    
    # Job operations
    def add_job(self, job_id: str, job_type: str, priority: int, job_request_payload: Union[Dict[str, Any], str], client_id: Optional[str] = None) -> Dict[str, Any]:
        """Add a job to the queue
        
        Args:
            job_id: Unique identifier for the job
            job_type: Type of job to be processed
            priority: Job priority (higher values have higher priority)
            job_request_payload: The payload/configuration from the client's job request (either a dict or JSON string)
            client_id: Optional client identifier
            
        Returns:
            Dict[str, Any]: Job data including position in queue
        """
        job_key = f"{JOB_PREFIX}{job_id}"
        
        # Ensure job_request_payload is a JSON string
        if isinstance(job_request_payload, dict):
            job_request_payload_json = json.dumps(job_request_payload)
        else:
            # Already a string, validate it's proper JSON
            try:
                # Parse and re-serialize to ensure it's valid JSON
                json.loads(job_request_payload)
                job_request_payload_json = job_request_payload
            except json.JSONDecodeError:
                # If not valid JSON, treat as a string and serialize it
                job_request_payload_json = json.dumps({"raw_payload": job_request_payload})
        
        # Store job details
        job_data = {
            "id": job_id,
            "type": job_type,
            "priority": priority,
            "job_request_payload": job_request_payload_json,
            "status": "pending",
            "created_at": time.time(),
        }
        
        if client_id:
            job_data["client_id"] = client_id
        
        self.client.hset(job_key, mapping=job_data)
        
        # Add to appropriate queue based on priority
        if priority > 0:
            self.client.zadd(PRIORITY_QUEUE, {job_id: priority})
        else:
            self.client.lpush(STANDARD_QUEUE, job_id)
        
        # Get queue position
        if priority > 0:
            position = self.client.zrank(PRIORITY_QUEUE, job_id)
        else:
            position = self.client.llen(STANDARD_QUEUE) - self.client.lpos(STANDARD_QUEUE, job_id)
        

        
        # Notify idle workers about the new job
        self.notify_idle_workers_of_job(job_id, job_type, job_request_payload=job_request_payload_json)
        
        # Return job data with position
        job_data["position"] = position if position is not None else -1
        return job_data
    
    # get_next_job method removed - legacy code
    
    def update_job_progress(self, job_id: str, progress: int, message: Optional[str] = None) -> bool:
        """Update the progress of a job.
        
        Args:
            job_id: ID of the job to update
            progress: Progress percentage (0-100)
            message: Optional status message
            
        Returns:
            bool: True if update was successful, False otherwise
        """
        # Get the worker_id from the job data
        job_key = f"{JOB_PREFIX}{job_id}"
        worker_id = self.client.hget(job_key, "worker_id")
        job_key = f"{JOB_PREFIX}{job_id}"
        
        # Check if job exists
        if not self.client.exists(job_key):

            return False
        
        # Update job progress
        self.client.hset(job_key, "progress", progress)
        if message:
            self.client.hset(job_key, "message", message)
        

        
        # Publish progress update event
        self.publish_job_update(job_id, "processing", progress=progress, message=message, worker_id=worker_id)
        
        return True
    
    def complete_job(self, job_id: str, result: Optional[Dict[str, Any]] = None) -> bool:
        """Mark a job as completed.
        
        Args:
            job_id: ID of the job to complete
            result: Optional job result data
            
        Returns:
            bool: True if completion was successful, False otherwise
        """
        # Get the worker_id from the job data
        job_key = f"{JOB_PREFIX}{job_id}"
        worker_id = self.client.hget(job_key, "worker_id")
        job_key = f"{JOB_PREFIX}{job_id}"
        
        # Check if job exists
        if not self.client.exists(job_key):

            return False
        
        # Update job status
        completed_at = time.time()
        self.client.hset(job_key, "status", "completed")
        self.client.hset(job_key, "completed_at", completed_at)
        
        # Add result if provided
        if result:
            self.client.hset(job_key, "result", json.dumps(result))
        

        
        # Publish completion event
        self.publish_job_update(job_id, "completed", result=result, worker_id=worker_id)
        
        return True
        
    def fail_job(self, job_id: str, error: str) -> bool:
        """Mark a job as failed.
        
        Args:
            job_id: ID of the job that failed
            error: Error message
            
        Returns:
            bool: True if failure was recorded successfully, False otherwise
        """
        # Get the worker_id from the job data
        job_key = f"{JOB_PREFIX}{job_id}"
        worker_id = self.client.hget(job_key, "worker_id")
        job_key = f"{JOB_PREFIX}{job_id}"
        
        # Check if job exists
        if not self.client.exists(job_key):

            return False
        
        # Update job status
        self.client.hset(job_key, "status", "failed")
        self.client.hset(job_key, "error", error if error else "Unknown error")
        

        
        # Publish failure event
        self.publish_job_update(job_id, "failed", error=error, worker_id=worker_id)
        
        return True
    
    def get_job_status(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Get the current status of a job
        
        Args:
            job_id: ID of the job to check
            
        Returns:
            Optional[Dict[str, Any]]: Job status data if job exists, None otherwise
        """
        job_key = f"{JOB_PREFIX}{job_id}"
        
        # Check if job exists
        if not self.client.exists(job_key):
            return None
        
        # Get job details from Redis
        redis_result = self.client.hgetall(job_key)
        
        # Create a new dictionary with the Redis result to ensure proper typing
        job_data: Dict[str, Any] = dict(redis_result)
        
        # Parse job_request_payload and result if present
        if "job_request_payload" in job_data:
            try:
                job_data["job_request_payload"] = json.loads(job_data["job_request_payload"])
            except:
                job_data["job_request_payload"] = {}
        # For backward compatibility, also check for the old "params" key
        elif "params" in job_data:
            try:
                job_data["job_request_payload"] = json.loads(job_data["params"])
                # Remove the old key to avoid confusion
                del job_data["params"]
            except:
                job_data["job_request_payload"] = {}
                
        if "result" in job_data:
            try:
                job_data["result"] = json.loads(job_data["result"])
            except:
                job_data["result"] = {}
        
        # Add queue position if job is pending
        if job_data.get("status") == "pending":
            priority = int(job_data.get("priority", 0))
            position: int = -1  # Default position if not found
            
            if priority > 0:
                # zrank can return None if the element is not in the sorted set
                rank_result = self.client.zrank(PRIORITY_QUEUE, job_id)
                if rank_result is not None:
                    position = int(rank_result)
            else:
                # Handle standard queue position calculation
                queue_length = int(self.client.llen(STANDARD_QUEUE))
                pos_result = self.client.lpos(STANDARD_QUEUE, job_id)
                if pos_result is not None:
                    position = queue_length - int(pos_result)
            
            # Add position to job data with explicit type
            job_data["position"] = position
        
        return job_data
    
    def register_worker(self, worker_id: str, capabilities: Optional[Dict[str, Any]] = None) -> bool:
        """Register a worker in Redis
        
        Args:
            worker_id: Unique identifier for the worker
            capabilities: Optional worker capabilities
            
        Returns:
            bool: True if registration was successful, False otherwise
        """
        worker_key = f"{WORKER_PREFIX}{worker_id}"
        
        # Initialize worker info from capabilities or create empty dict
        worker_info = capabilities or {}
        
        # Add worker info with current timestamp
        worker_info["registered_at"] = time.time()
        worker_info["last_heartbeat"] = time.time()
        worker_info["status"] = "idle"
        
        # Add worker to hash storage
        self.client.hset(worker_key, mapping=worker_info)
        
        # Add worker to tracking sets
        self.client.sadd("workers:all", worker_id)
        
        # Add to idle workers set since all workers start as idle
        self.client.sadd("workers:idle", worker_id)
        
        return True
        
    def worker_heartbeat(self, worker_id: str) -> bool:
        """
        Record a heartbeat from a worker.
        
        Args:
            worker_id: ID of the worker sending the heartbeat
            
        Returns:
            bool: True if heartbeat was recorded successfully, False otherwise
        """
        worker_key = f"{WORKER_PREFIX}{worker_id}"
        
        # Check if worker exists
        if not self.client.exists(worker_key):
            return False
        
        # Update heartbeat timestamp
        self.client.hset(worker_key, "last_heartbeat", time.time())
        return True
        
    def update_worker_heartbeat(self, worker_id: str, status: Optional[str] = None) -> bool:
        # Using Optional[str] instead of str = None to comply with PEP 484
        """Update worker heartbeat timestamp and optionally status"""
        worker_key = f"{WORKER_PREFIX}{worker_id}"
        
        # Check if worker exists
        if not self.client.exists(worker_key):
            return False
        
        # Update heartbeat
        update_data = {"last_heartbeat": str(time.time())}
        
        # Update status if provided
        if status is not None:
            # Get current status for comparison
            current_status = self.client.hget(worker_key, "status")
            # Ensure status is treated as a string type
            update_data["status"] = str(status)
            
            # Update worker tracking sets based on status change
            if status == "idle":
                # Worker is now idle, add to idle workers set
                self.client.sadd("workers:idle", worker_id)

            elif current_status == "idle" and status != "idle":
                # Worker was idle but now is not, remove from idle workers set
                self.client.srem("workers:idle", worker_id)

            

        
        # Update the worker hash
        self.client.hset(worker_key, mapping=update_data)
        return True
        
    def worker_exists(self, worker_id: str) -> bool:
        """Check if a worker exists in Redis"""
        worker_key = f"{WORKER_PREFIX}{worker_id}"
        exists = self.client.exists(worker_key)
        
        if exists:
            # Also check if worker is in the all workers set as a safeguard
            in_all_set = self.client.sismember("workers:all", worker_id)
            if not in_all_set:
                self.client.sadd("workers:all", worker_id)
            
            return True
        return False
    
    def update_worker_status(self, worker_id: str, status: str, job_id: Optional[str] = None) -> bool:
        """Update the status of a worker.
        
        Args:
            worker_id: ID of the worker to update
            status: New status (e.g., "idle", "busy")
            job_id: Optional ID of the job the worker is processing
            
        Returns:
            bool: True if update was successful, False otherwise
        """
        worker_key = f"{WORKER_PREFIX}{worker_id}"
        
        # Check if worker exists
        if not self.client.exists(worker_key):
            return False
        
        # Get current status for comparison
        current_status = self.client.hget(worker_key, "status")
        
        # Update status in the worker hash
        update_data = {"status": status}
        
        # Update job_id if provided
        if job_id is not None:
            update_data["current_job_id"] = job_id
        elif status == "idle" and self.client.hexists(worker_key, "current_job_id"):
            # If worker is now idle and no job_id is provided, remove current_job_id
            self.client.hdel(worker_key, "current_job_id")
            
        self.client.hset(worker_key, mapping=update_data)
        
        # Update worker tracking sets based on status change
        if status == "idle":
            # Worker is now idle, add to idle workers set
            self.client.sadd("workers:idle", worker_id)

        elif current_status == "idle" and status != "idle":
            # Worker was idle but now is not, remove from idle workers set
            self.client.srem("workers:idle", worker_id)

        

        
        return True
        
    def update_worker_capabilities(self, worker_id: str, capabilities: Dict[str, Any]) -> bool:
        """Update worker capabilities"""
        worker_key = f"{WORKER_PREFIX}{worker_id}"
        
        # Check if worker exists
        if not self.client.exists(worker_key):
            return False
        
        # Convert capabilities dict to JSON string
        capabilities_json = json.dumps(capabilities)
        
        # Update capabilities in the worker hash
        self.client.hset(worker_key, "capabilities", capabilities_json)
        

        
        return True
    
    def get_worker_info(self, worker_id: str) -> Optional[Dict[str, Any]]:
        """Get information about a worker
        
        Args:
            worker_id: ID of the worker to get information about
            
        Returns:
            Optional[Dict[str, Any]]: Worker information if worker exists, None otherwise
        """
        worker_key = f"{WORKER_PREFIX}{worker_id}"
        
        # Check if worker exists
        if not self.client.exists(worker_key):
            return None
        
        # Get worker details from Redis
        redis_result = self.client.hgetall(worker_key)
        
        # Create a new dictionary with the Redis result to ensure proper typing
        worker_data: Dict[str, Any] = dict(redis_result)
        
        return worker_data
    
    def get_stats(self) -> Dict[str, Any]:
        """Get system statistics
        
        Returns:
            Dict[str, Any]: Dictionary containing statistics about queues, jobs, and workers
            with the following structure:
            {
                "queues": {
                    "priority": int,  # Number of jobs in priority queue
                    "standard": int,  # Number of jobs in standard queue
                    "total": int      # Total number of jobs in all queues
                },
                "jobs": {
                    "total": int,     # Total number of jobs
                    "status": {       # Counts of jobs by status
                        "<status>": int
                    }
                },
                "workers": {
                    "total": int,     # Total number of workers
                    "status": {       # Counts of workers by status
                        "<status>": int
                    }
                }
            }
        """
        # Initialize stats structure with explicit type annotations
        stats: Dict[str, Dict[str, Any]] = {
            "queues": {
                "priority": 0,
                "standard": 0,
                "total": 0
            },
            "jobs": {
                "total": 0,
                "status": {}
            },
            "workers": {
                "total": 0,
                "status": {}
            }
        }
        
        try:
            # Queue stats - ensure proper type conversion from Redis return values
            priority_count: int = int(self.client.zcard(PRIORITY_QUEUE))
            standard_count: int = int(self.client.llen(STANDARD_QUEUE))
            
            stats["queues"]["priority"] = priority_count
            stats["queues"]["standard"] = standard_count
            stats["queues"]["total"] = priority_count + standard_count
            
            # Job stats
            job_keys = self.client.keys(f"{JOB_PREFIX}*")
            stats["jobs"]["total"] = len(job_keys)
            
            # Job status counts
            for job_key in job_keys:
                job_status = self.client.hget(job_key, "status")
                if job_status:
                    # Ensure job_status is treated as a string for dictionary key
                    status_key = str(job_status)
                    current_count = stats["jobs"]["status"].get(status_key, 0)
                    stats["jobs"]["status"][status_key] = current_count + 1
            
            # Worker stats
            worker_keys = self.client.keys(f"{WORKER_PREFIX}*")
            stats["workers"]["total"] = len(worker_keys)
            
            # Worker status counts
            for worker_key in worker_keys:
                worker_status = self.client.hget(worker_key, "status")
                if worker_status:
                    # Ensure worker_status is treated as a string for dictionary key
                    status_key = str(worker_status)
                    current_count = stats["workers"]["status"].get(status_key, 0)
                    stats["workers"]["status"][status_key] = current_count + 1
            
        except Exception:
            # Keep the empty stats structure in case of errors
            pass
        
        return stats
    
    def cleanup_stale_jobs(self, max_heartbeat_age: int = 600) -> int:
        """Clean up stale jobs from workers that have disappeared"""
        current_time = time.time()
        stale_count = 0
        
        # Get all worker information
        worker_keys = self.client.keys(f"{WORKER_PREFIX}*")
        
        for worker_key in worker_keys:
            worker_id = worker_key.replace(f"{WORKER_PREFIX}", "")
            # Get worker details from Redis and convert to proper dictionary
            redis_result = self.client.hgetall(worker_key)
            worker_data: Dict[str, Any] = dict(redis_result)
            
            # Skip if no heartbeat data
            if "last_heartbeat" not in worker_data:
                continue
                
            last_heartbeat = float(worker_data["last_heartbeat"])
            heartbeat_age = current_time - last_heartbeat
            
            # Check if worker is stale
            if heartbeat_age > max_heartbeat_age:

                
                # Find any jobs assigned to this worker
                job_keys = self.client.keys(f"{JOB_PREFIX}*")
                
                for job_key in job_keys:
                    job_id = job_key.replace(f"{JOB_PREFIX}", "")
                    job_worker = self.client.hget(job_key, "worker")
                    job_status = self.client.hget(job_key, "status")
                    
                    # Only reset jobs that are in processing status and assigned to this worker
                    if job_worker == worker_id and job_status == "processing":
                        # Get job priority
                        priority = int(self.client.hget(job_key, "priority") or 0)
                        
                        # Reset job status to pending
                        self.client.hset(job_key, "status", "pending")
                        self.client.hdel(job_key, "worker")
                        self.client.hdel(job_key, "started_at")
                        
                        # Add back to queue
                        if priority > 0:
                            self.client.zadd(PRIORITY_QUEUE, {job_id: priority})
                        else:
                            self.client.lpush(STANDARD_QUEUE, job_id)
                            

                        stale_count += 1
                
                # Mark worker as disconnected
                self.client.hset(worker_key, "status", "disconnected")
        
        return stale_count
    
    def mark_stale_workers_out_of_service(self, max_heartbeat_age: int = 120) -> int:
        """Mark workers without recent heartbeats as out_of_service
        
        Args:
            max_heartbeat_age: Maximum age of heartbeat in seconds (default: 120 seconds / 2 minutes)
            
        Returns:
            Number of workers marked as out_of_service
        """
        current_time = time.time()
        stale_count = 0
        worker_keys = self.client.keys(f"{WORKER_PREFIX}*")
        
        for worker_key in worker_keys:
            worker_id = worker_key.replace(f"{WORKER_PREFIX}", "")
            worker_status = self.client.hget(worker_key, "status")
            last_heartbeat = float(self.client.hget(worker_key, "last_heartbeat") or 0)
            heartbeat_age = current_time - last_heartbeat
            
            # Skip workers already marked as out_of_service or disconnected
            if worker_status in ["out_of_service", "disconnected"]:
                continue
                
            # Check if heartbeat is stale (older than max_heartbeat_age)
            if heartbeat_age > max_heartbeat_age:
                # Mark worker as out_of_service
                self.client.hset(worker_key, "status", "out_of_service")

                stale_count += 1
        
        return stale_count
        
    def notify_idle_workers_of_job(self, job_id: str, job_type: str, job_request_payload: Optional[str] = None) -> List[str]:
        # Using Optional[Dict[str, Any]] to match interface requirements
        """
        Notify idle workers about an available job.
        
        This method uses the workers:idle Redis set to find all idle workers
        and notify them about an available job. It does not perform any heartbeat
        checks or worker health monitoring, as those concerns are handled by
        separate background processes.
        
        Args:
            job_id: Unique identifier for the job
            job_type: Type of job to be processed
            job_request_payload: Optional payload from the original job request as a JSON string
            
        Returns:
            List[str]: List of worker IDs that were notified
        """
        # Get all idle workers directly from the Redis set
        idle_workers = self.client.smembers("workers:idle")
        
        # Publish notification to job channel
        notification = {
            "type": "job_available",
            "job_id": job_id,
            "job_type": job_type
        }
        
        if job_request_payload:
            # Include the original job request payload in the notification
            notification["job_request_payload"] = job_request_payload
            
        self.client.publish("job_notifications", json.dumps(notification))
        

        return list(idle_workers)
    
    def claim_job(self, job_id: str, worker_id: str, claim_timeout: int = 30) -> Optional[Dict[str, Any]]:
        """Atomically claim a job with a timeout"""
        job_key = f"{JOB_PREFIX}{job_id}"
        
        # Use Redis transaction to ensure atomicity
        with self.client.pipeline() as pipe:
            try:
                # Watch the job status to ensure it's still pending
                pipe.watch(job_key)
                job_status = pipe.hget(job_key, "status")
                
                if job_status != "pending":
                    pipe.unwatch()

                    return None
                
                # Start transaction
                pipe.multi()
                
                # Update job status to claimed with timeout
                pipe.hset(job_key, "status", "claimed")
                pipe.hset(job_key, "worker", worker_id)
                pipe.hset(job_key, "claimed_at", time.time())
                pipe.hset(job_key, "claim_timeout", claim_timeout)
                
                # Execute transaction
                pipe.execute()
                
                # Get job details from Redis and convert to proper dictionary
                redis_result = self.client.hgetall(job_key)
                job_data: Dict[str, Any] = dict(redis_result)
                
                # Parse params back to dict
                if "params" in job_data:
                    # Convert the JSON string to a dictionary
                    params_str = job_data["params"]
                    if isinstance(params_str, str):
                        job_data["params"] = json.loads(params_str)
                

                return job_data
                
            except Exception as e:

                return None
    
    def cleanup_stale_claims(self, max_claim_age: int = 60) -> int:
        """Reset jobs that were claimed but never started processing"""
        current_time = time.time()
        stale_count = 0
        
        # Find claimed jobs
        job_keys = self.client.keys(f"{JOB_PREFIX}*")
        
        for job_key in job_keys:
            job_id = job_key.replace(f"{JOB_PREFIX}", "")
            job_status = self.client.hget(job_key, "status")
            
            # Only check claimed jobs
            if job_status == "claimed":
                claimed_at = float(self.client.hget(job_key, "claimed_at") or 0)
                claim_age = current_time - claimed_at
                claim_timeout = int(self.client.hget(job_key, "claim_timeout") or 30)
                
                # Check if claim is stale
                if claim_age > claim_timeout:
                    # Get job priority
                    priority = int(self.client.hget(job_key, "priority") or 0)
                    
                    # Reset job status to pending
                    self.client.hset(job_key, "status", "pending")
                    self.client.hdel(job_key, "worker")
                    self.client.hdel(job_key, "claimed_at")
                    self.client.hdel(job_key, "claim_timeout")
                    
                    # Add back to queue
                    if priority > 0:
                        self.client.zadd(PRIORITY_QUEUE, {job_id: priority})
                    else:
                        self.client.lpush(STANDARD_QUEUE, job_id)
                        

                    stale_count += 1
        
        return stale_count
    
    # Redis pub/sub methods
    def publish_job_update(self, job_id: str, status: str, **kwargs) -> bool:
        """Publish a job update event"""
        try:
            # Create update message
            message = {
                "job_id": job_id,
                "status": status,
                "timestamp": time.time(),
                **kwargs
            }
            
            # Publish to job-specific channel
            self.client.publish(f"job_updates:{job_id}", json.dumps(message))
            
            # Also publish to global job updates channel
            self.client.publish("job_updates", json.dumps(message))
            
            return True
        except Exception as e:

            return False
    
    async def subscribe_to_channel(self, channel: str, callback: Callable[[Dict[str, Any]], None]) -> None:
        """Subscribe to a Redis channel for updates"""
        if not self.async_client:
            await self.connect_async()
        
        # Ensure pubsub is not None before attempting to subscribe
        if self.pubsub:
            await self.pubsub.subscribe(**{channel: callback})

        
    def get_all_workers_status(self) -> Dict[str, Dict[str, Any]]:
        """Get status information for all workers
        
        Returns:
            Dictionary with worker IDs as keys and worker status information as values
        """
        workers_status = {}
        
        try:
            # Get all worker keys
            worker_keys = self.client.keys(f"{WORKER_PREFIX}*")
            
            for worker_key in worker_keys:
                worker_id = worker_key.replace(f"{WORKER_PREFIX}", "")
                # Get worker details from Redis and convert to proper dictionary
                redis_result = self.client.hgetall(worker_key)
                worker_data: Dict[str, Any] = dict(redis_result)
                
                # Add additional calculated fields
                if "last_heartbeat" in worker_data:
                    last_heartbeat = float(worker_data["last_heartbeat"])
                    worker_data["heartbeat_age"] = time.time() - last_heartbeat
                
                # Add to result dictionary
                workers_status[worker_id] = worker_data
                

            
        except Exception:
            pass
            
        return workers_status
