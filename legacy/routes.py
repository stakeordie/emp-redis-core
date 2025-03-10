#!/usr/bin/env python3
# Core WebSocket routes for the Redis queue system
# mypy: ignore-errors
import json
import asyncio
import uuid
import time
from typing import Dict, Any, Optional, List, Union, Callable
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from websockets.exceptions import ConnectionClosedError
from .utils.logger import logger

from .models import (
    MessageType, parse_message, 
    SubmitJobMessage, GetJobStatusMessage, RegisterWorkerMessage, 
    UpdateJobProgressMessage, CompleteJobMessage, GetStatsMessage, SubscribeStatsMessage,
    SubscribeJobMessage, JobAcceptedMessage, JobStatusMessage, JobUpdateMessage, JobAssignedMessage,
    JobCompletedMessage, ErrorMessage, WorkerRegisteredMessage, WorkerHeartbeatMessage, ClaimJobMessage,
    JobClaimedMessage, JobAvailableMessage,
    WorkerStatusMessage
)
from .connection_manager import ConnectionManager
from .redis_service import RedisService, STANDARD_QUEUE, PRIORITY_QUEUE

# Initialize services
# Using definite assignment operator to indicate these are never None
redis_service: RedisService = RedisService()
connection_manager: ConnectionManager = ConnectionManager()

# Message handler maps
# Maps message types to their handler functions
client_message_handlers: Dict[str, Callable[[str, Dict[str, Any]], Any]] = {}
worker_message_handlers: Dict[str, Callable[[str, Any], Any]] = {}

# Task to periodically broadcast system stats to subscribed clients
async def broadcast_stats_task():
    """Periodically fetch stats from Redis and broadcast to subscribed clients"""
    last_stats = None  # Track previous stats to detect changes
    
    while True:
        try:
            # Check if there are any clients subscribed or monitors connected
            subscribers_count = len(connection_manager.stats_subscriptions)
            monitors_count = len(connection_manager.monitor_connections)
            
            # Skip if no clients are subscribed AND no monitors are connected
            if subscribers_count == 0 and monitors_count == 0:
                await asyncio.sleep(3)  # Check less frequently if no subscribers
                continue
                
            # Get stats from Redis
            stats = redis_service.get_stats()
            
            # Verify if we got valid stats
            if not stats:
                await asyncio.sleep(1)
                continue
                
            # Sanitize stats data (convert string values to integers)
            try:
                sanitized_stats = {
                    'type': MessageType.STATS_RESPONSE,
                    'queues': {
                        'priority': int(stats['queues']['priority']),
                        'standard': int(stats['queues']['standard']),
                        'total': int(stats['queues']['total'])
                    },
                    'jobs': {
                        'total': int(stats['jobs']['total']),
                        'status': {
                            k: int(v) for k, v in stats['jobs']['status'].items()
                        }
                    },
                    'workers': {
                        'total': int(stats['workers']['total']),
                        'status': {
                            k: int(v) for k, v in stats['workers']['status'].items()
                        }
                    }
                }
                
                # Check if stats have changed since last broadcast
                is_changed = last_stats != sanitized_stats
                
                # Only broadcast if stats changed or every 5 seconds regardless
                if is_changed or not last_stats or time.time() % 5 < 1:
                    response = sanitized_stats
                    await connection_manager.broadcast_stats(response)
                    
                    # Store current stats for future comparison
                    last_stats = sanitized_stats
                
            except Exception:
                pass
            
        except Exception:
            pass
        
        # Wait before sending the next update
        await asyncio.sleep(1)
        
# Task to periodically clean up stale job claims
async def cleanup_stale_claims_task():
    """Periodically check for and reset stale job claims"""
    
    while True:
        try:
            # Check for stale claims every 15 seconds
            redis_service.cleanup_stale_claims()
                
        except Exception:
            pass
        
        # Wait before next cleanup
        await asyncio.sleep(15)

# Task to periodically mark stale workers as out_of_service
async def mark_stale_workers_task():
    """Periodically check for and mark workers without recent heartbeats as out_of_service"""
    
    while True:
        try:
            # Check for stale workers every 30 seconds
            redis_service.mark_stale_workers_out_of_service(max_heartbeat_age=120)  # 2 minutes
                
        except Exception:
            pass
        
        # Wait before next check
        await asyncio.sleep(30)

# Task to periodically send system status updates to monitors
async def monitor_status_update_task():
    """Periodically send comprehensive system status updates to all connected monitors"""
    
    while True:
        try:
            # Only proceed if there are connected monitors
            if connection_manager.monitor_connections:
                # Use the enhanced send_system_status_to_monitors method to send comprehensive status
                # This will include detailed worker status from Redis
                await connection_manager.send_system_status_to_monitors(redis_service)
        except Exception:
            pass
        
        # Wait before sending the next update (15 seconds)
        await asyncio.sleep(15)

def init_routes(app: FastAPI, redis_service: Optional[RedisService] = None, connection_manager: Optional[ConnectionManager] = None) -> None:
    """Initialize WebSocket routes with optional service dependencies
    
    Args:
        app: The FastAPI application
        redis_service: Optional Redis service instance (if not provided, uses global instance)
        connection_manager: Optional connection manager instance (if not provided, uses global instance)
    """
    # Use provided services or fall back to global instances
    # We need to handle the global variable shadowing issue
    global_redis_service = globals().get("redis_service")
    global_connection_manager = globals().get("connection_manager")
    
    # If parameters are provided, use them; otherwise fall back to global instances
    redis_service_env = redis_service or global_redis_service
    connection_manager_env = connection_manager or global_connection_manager
    
    # Update the global references for use within route handlers
    globals()["redis_service"] = redis_service_env
    globals()["connection_manager"] = connection_manager_env
    
    # Start background tasks
    app.add_event_handler("startup", broadcast_stats_task)
    app.add_event_handler("startup", cleanup_stale_claims_task)
    app.add_event_handler("startup", mark_stale_workers_task)
    app.add_event_handler("startup", monitor_status_update_task)
    app.add_event_handler("startup", start_redis_listener)
    
    # Log when the application is fully started
    @app.on_event("startup")
    async def on_startup():
        print("\n\n===================================================================")
        print("=== 🚀 WebSocket server fully initialized and ready for connections! ====")
        print("===================================================================\n\n")
        logger.info("WebSocket server fully initialized and ready to accept connections")

    @app.websocket("/ws/client/{client_id}")
    async def client_websocket(websocket: WebSocket, client_id: str):
        """WebSocket endpoint for clients"""
        # Print more visible connection logs
        print(f"\n\n==== CLIENT CONNECTION ATTEMPT: {client_id} ====\n")
        logger.info(f"New client WebSocket connection attempt: {client_id}")
        
        # Accept the connection
        try:
            await websocket.accept()
            connection_manager.client_connections[client_id] = websocket
            logger.info(f"Client WebSocket connected successfully: {client_id}")
            
            # Send an immediate welcome message
            welcome_message = {"type": "connection_established", "message": f"Welcome {client_id}! Connected to Redis Hub"}
            await websocket.send_text(json.dumps(welcome_message))
            
            print(f"\n==== CLIENT CONNECTED: {client_id} ====\n")
        except Exception as e:
            logger.error(f"Error accepting client connection {client_id}: {str(e)}")
            return
        
        try:
            while True:
                # Receive message from client
                message_text = await websocket.receive_text()
                
                # Process client message
                
                # Parse message
                try:
                    message_data = json.loads(message_text)
                    # Process the parsed message data
                    
                    # Skip Pydantic model conversion and work directly with the parsed JSON data
                    message_type = message_data.get('type')
                    
                    if not message_type:
                        # Handle invalid message - missing type
                        error_message = ErrorMessage(error="Invalid message format - missing type")
                        await connection_manager.send_to_client(client_id, error_message)
                        continue
                    
                    # Process message based on type
                    
                    if message_type == MessageType.SUBMIT_JOB:
                        # Handle job submission
                        await handle_submit_job(client_id, message_data)
                    elif message_type == MessageType.GET_JOB_STATUS:
                        job_id = message_data.get('job_id', '')
                        await handle_get_job_status(client_id, job_id)
                    elif message_type == MessageType.SUBSCRIBE_JOB:
                        job_id = message_data.get('job_id', '')
                        await handle_subscribe_job(client_id, job_id)
                    elif message_type == MessageType.SUBSCRIBE_STATS:
                        handle_subscribe_stats(client_id)
                    elif message_type == MessageType.GET_STATS:
                        await handle_get_stats(client_id)
                    else:
                        # Handle unrecognized message type
                        error_message = ErrorMessage(error=f"Unsupported message type: {message_type}")
                        await connection_manager.send_to_client(client_id, error_message)
                        
                except json.JSONDecodeError:
                    # Handle invalid JSON
                    error_message = ErrorMessage(error="Invalid JSON format")
                    await connection_manager.send_to_client(client_id, error_message)
                    
                except Exception as e:
                    # Handle processing error
                    error_message = ErrorMessage(error="Server error processing message")
                    await connection_manager.send_to_client(client_id, error_message)
                
        except WebSocketDisconnect:
            connection_manager.disconnect_client(client_id)
        
        except Exception as e:
            # Handle unexpected error
            connection_manager.disconnect_client(client_id)
    
    @app.websocket("/ws/monitor/{monitor_id}")
    async def monitor_websocket(websocket: WebSocket, monitor_id: str):
        """WebSocket endpoint for monitors"""
        # Handle monitor connection
        
        # Accept the connection
        try:
            await websocket.accept()
            await connection_manager.connect_monitor(websocket, monitor_id)
            # Send an immediate welcome message
            welcome_message = {"type": "connection_established", "message": f"Welcome Monitor {monitor_id}! Connected to Redis Hub"}
            await websocket.send_text(json.dumps(welcome_message))
            
            # Send initial system status using the enhanced method
            # This will include detailed worker status from Redis
            await connection_manager.send_system_status_to_monitors(redis_service)
            
        except Exception as e:
            # Handle connection error
            return
        
        try:
            while True:
                # Receive message from monitor
                message_text = await websocket.receive_text()
                
                # Parse message
                try:
                    message_data = json.loads(message_text)
                    # Process monitor message
                    
                    # Process message based on type
                    message_type = message_data.get("type")
                    
                    if message_type == "subscribe":
                        # Handle subscription request
                        channels = message_data.get("channels", [])
                        connection_manager.set_monitor_subscriptions(monitor_id, channels)
                        await websocket.send_text(json.dumps({
                            "type": "subscription_confirmed",
                            "channels": channels
                        }))
                        # Handle subscription confirmation
                    elif message_type == "get_system_status":
                        # Send immediate system status update using the enhanced method
                        # This will include detailed worker status from Redis
                        await connection_manager.send_system_status_to_monitors(redis_service)
                    elif message_type == "stay_alive":
                        # Respond to stay_alive to keep connection alive
                        await websocket.send_text(json.dumps({
                            "type": "stay_alive_response",
                            "timestamp": time.time()
                        }))
                    else:
                        # Handle unrecognized message type
                        await websocket.send_text(json.dumps({
                            "type": "error",
                            "error": f"Unsupported message type: {message_type}"
                        }))
                        
                except json.JSONDecodeError:
                    # Handle invalid JSON
                    await websocket.send_text(json.dumps({
                        "type": "error",
                        "error": "Invalid JSON format"
                    }))
                    
                except Exception as e:
                    # Handle processing error
                    await websocket.send_text(json.dumps({
                        "type": "error",
                        "error": "Server error processing message"
                    }))
                
        except WebSocketDisconnect:
            connection_manager.disconnect_monitor(monitor_id)
            # Handle monitor disconnect
        
        except Exception as e:
            # Handle unexpected error
            connection_manager.disconnect_monitor(monitor_id)
    
    @app.websocket("/ws/worker/{worker_id}")
    async def worker_websocket(websocket: WebSocket, worker_id: str):
        """WebSocket endpoint for workers"""
        # Handle worker connection
        
        # Accept the connection
        try:
            # Accept the connection
            await websocket.accept()
            
            # Store the connection
            connection_manager.worker_connections[worker_id] = websocket
            connection_manager.worker_status[worker_id] = "idle"  # Set initial status to idle
            
            # Send an immediate welcome message
            welcome_message = {"type": "connection_established", "message": f"Welcome Worker {worker_id}! Connected to Redis Hub"}
            logger.debug(f"[WORKER-CONN] Sending welcome message to worker {worker_id}")
            print(f"[WORKER-CONN] Sending welcome message: {welcome_message}")
            await websocket.send_text(json.dumps(welcome_message))
            print(f"[WORKER-CONN] Welcome message sent successfully")
            
            print(f"\n==== WORKER CONNECTED: {worker_id} ====\n")
        except WebSocketDisconnect as e:
            logger.error(f"WebSocket disconnected during worker connection setup for {worker_id}: {str(e)}")
            print(f"[WORKER-CONN-ERROR] WebSocket disconnected during setup: {str(e)}")
            return
        except ConnectionClosedError as e:
            logger.error(f"Connection closed during worker connection setup for {worker_id}: {str(e)}")
            print(f"[WORKER-CONN-ERROR] Connection closed during setup: {str(e)}")
            return
        except Exception as e:
            logger.error(f"Error accepting worker connection {worker_id}: {str(e)}")
            print(f"[WORKER-CONN-ERROR] Unexpected error: {str(e)}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            print(f"[WORKER-CONN-ERROR] Traceback: {traceback.format_exc()}")
            return
        
        try:
            while True:
                # Receive message from worker
                message_text = await websocket.receive_text()
                
                # Log raw message for debugging
                print(f"\n\n🔍🔍🔍 RAW WORKER MESSAGE RECEIVED: {message_text}")
                print(f"🔍 Worker ID: {worker_id}")
                print(f"🔍 Message length: {len(message_text)} bytes")
                logger.info(f"[WORKER-MSG-RAW] Received raw message from worker {worker_id}: {message_text}")
                
                # Parse message
                try:
                    message_data = json.loads(message_text)
                    print(f"🔍 Parsed JSON: {json.dumps(message_data)}")
                    logger.info(f"[WORKER-MSG-PARSED] Parsed message data: {message_data}")
                    message = parse_message(message_data)
                    
                    if not message:
                        logger.error(f"Invalid message received from worker {worker_id}")
                        error_message = ErrorMessage(error="Invalid message format")
                        await connection_manager.send_to_worker(worker_id, error_message)
                        continue
                        
                    # Process message based on type
                    if message.type == MessageType.REGISTER_WORKER:
                        # Cast message to the expected type for type checking
                        typed_message = RegisterWorkerMessage(**message.dict())
                        await handle_register_worker(worker_id, typed_message)
                    # GET_NEXT_JOB handler removed - legacy code
                    elif message.type == MessageType.UPDATE_JOB_PROGRESS:
                        # Cast message to the expected type for type checking
                        typed_message = UpdateJobProgressMessage(**message.dict())
                        await handle_update_job_progress(worker_id, typed_message)
                    elif message.type == MessageType.COMPLETE_JOB:
                        # Cast message to the expected type for type checking
                        typed_message = CompleteJobMessage(**message.dict())
                        await handle_complete_job(worker_id, typed_message)
                    # New message handlers for push-based notification system
                    elif message.type == MessageType.WORKER_HEARTBEAT:
                        # Cast message to the expected type for type checking
                        typed_message = WorkerHeartbeatMessage(**message.dict())
                        await handle_worker_heartbeat(worker_id, typed_message)
                    # HEARTBEAT handler removed - legacy code
                    elif message.type == MessageType.WORKER_STATUS:
                        # Cast message to the expected type for type checking
                        typed_message = WorkerStatusMessage(**message.dict())
                        await handle_worker_status(worker_id, typed_message)
                    elif message.type == MessageType.CLAIM_JOB:
                        # Cast message to the expected type for type checking
                        typed_message = ClaimJobMessage(**message.dict())
                        await handle_claim_job(worker_id, typed_message)
                    # SUBSCRIBE_JOB_NOTIFICATIONS handler removed - redundant functionality
                    else:
                        # Handle unrecognized message type
                        error_message = ErrorMessage(error=f"Unsupported message type: {message.type}")
                        await connection_manager.send_to_worker(worker_id, error_message)
                        
                except json.JSONDecodeError:
                    # Handle invalid JSON
                    error_message = ErrorMessage(error="Invalid JSON format")
                    await connection_manager.send_to_worker(worker_id, error_message)
                    
                except Exception as e:
                    # Handle processing error
                    error_message = ErrorMessage(error="Server error processing message")
                    await connection_manager.send_to_worker(worker_id, error_message)
                
        except WebSocketDisconnect:
            connection_manager.disconnect_worker(worker_id)
            # Mark worker as disconnected in Redis
            redis_service.update_worker_status(worker_id, "disconnected")
        
        except Exception as e:
            # Handle unexpected error
            connection_manager.disconnect_worker(worker_id)
            # Mark worker as disconnected in Redis
            redis_service.update_worker_status(worker_id, "disconnected")

    # Client message handlers
    async def handle_submit_job(client_id: str, message_data: Dict[str, Any]):
        """Handle job submission from a client"""
        # Generate job ID if not provided
        job_id = f"job-{uuid.uuid4()}"
        
        # Extract data directly from the dictionary
        job_type = message_data.get('job_type', 'unknown')
        priority = message_data.get('priority', 0)
        payload = message_data.get('payload', {})
        
        # Process job submission
        
        # Add job to Redis
        job_data = redis_service.add_job(
            job_id=job_id,
            job_type=job_type,
            priority=priority,
            job_request_payload=payload,
            client_id=client_id
        )
        
        # Job submitted
        
        # Create job available notification for workers
        notification = JobAvailableMessage(
            job_id=job_id,
            job_type=job_type,
            priority=priority,
            job_request_payload=payload
        )
        
        # Send notification to idle workers and get count
        worker_count = await connection_manager.notify_idle_workers(notification)
        # Notify workers
        
        # Send confirmation response with worker count
        response = JobAcceptedMessage(
            job_id=job_id,
            position=job_data.get("position", -1),
            notified_workers=worker_count  # Include the count of notified workers
        )
        
        success = await connection_manager.send_to_client(client_id, response)
        # Automatically subscribe client to job updates
        await connection_manager.subscribe_to_job(client_id, job_id)
    
    async def handle_get_job_status(client_id: str, job_id: str):
        """Handle job status request from a client"""
        # Handle job status request
        
        # Get job status from Redis
        job_data = redis_service.get_job_status(job_id)
        
        if not job_data:
            error_message = ErrorMessage(error=f"Job {job_id} not found")
            await connection_manager.send_to_client(client_id, error_message)
            return
        
        # Create response
        response = JobStatusMessage(
            job_id=job_id,
            status=job_data.get("status", "unknown"),
            progress=int(job_data.get("progress", 0)) if "progress" in job_data else None,
            worker_id=job_data.get("worker"),
            started_at=float(job_data.get("started_at")) if "started_at" in job_data else None,
            completed_at=float(job_data.get("completed_at")) if "completed_at" in job_data else None,
            result=job_data.get("result"),
            message=job_data.get("message")
        )
        await connection_manager.send_to_client(client_id, response)
        
        # Subscribe client to future updates for this job
        connection_manager.subscribe_to_job(job_id, client_id)
    
    async def handle_subscribe_job(client_id: str, job_id: str):
        """Handle job subscription request from a client"""
        # Handle job subscription
        await connection_manager.subscribe_to_job(job_id, client_id)
    
    def handle_subscribe_stats(client_id: str):
        """Handle stats subscription request from a client"""
        # Handle stats subscription
        connection_manager.subscribe_to_stats(client_id)
    
    async def handle_get_stats(client_id: str):
        """Handle stats request from a client"""
        stats = redis_service.get_stats()
        
        # Format the stats as a proper message with a type field
        stats_response = {
            "type": MessageType.STATS_RESPONSE,
            "stats": stats,
            "timestamp": time.time()
        }
        
        await connection_manager.send_to_client(client_id, stats_response)
    
    # Worker message handlers
    async def handle_register_worker(worker_id: str, message: RegisterWorkerMessage):
        """Handle worker registration"""
        # Get worker info from message
        worker_info = {
            "machine_id": message.machine_id,
            "gpu_id": message.gpu_id
        }
        
        # Register worker in Redis
        registration_success = redis_service.register_worker(worker_id, worker_info)
        
        # Send confirmation
        response = WorkerRegisteredMessage(worker_id=worker_id)
        await connection_manager.send_to_worker(worker_id, response)
    
    # handle_get_next_job function removed - legacy code
    
    async def handle_update_job_progress(worker_id: str, message: UpdateJobProgressMessage):
        """Handle worker job progress update"""
        job_id = message.job_id
        progress = message.progress
        status_message = message.message
        
        print(f"\n\n🟣🟣🟣 JOB PROGRESS UPDATE 🟣🟣🟣")
        print(f"🟣 Worker: {worker_id}")
        print(f"🟣 Job ID: {job_id}")
        print(f"🟣 Progress: {progress}%")
        print(f"🟣 Message: {status_message}")
        
        logger.info(f"[JOB-PROGRESS] Worker {worker_id} reported progress for job {job_id}: {progress}%")
        
        # Update worker heartbeat
        redis_service.update_worker_heartbeat(worker_id)
        
        # Update job progress in Redis
        # The method signature is update_job_progress(job_id, progress, message=None)
        redis_service.update_job_progress(job_id, progress, status_message)
        
        # Create update message for client
        update = JobUpdateMessage(
            job_id=job_id,
            status="processing",
            progress=progress,
            message=status_message
        )
        
        # Send update to subscribed client
        client_notified = await connection_manager.send_job_update(job_id, update)
    
    async def handle_complete_job(worker_id: str, message: CompleteJobMessage):
        """Handle worker job completion notification"""
        job_id = message.job_id
        result = message.result
        
        # Handle job completion
        
        # Update worker heartbeat
        redis_service.update_worker_heartbeat(worker_id)
        
        # Mark job as completed in Redis
        # The method signature is complete_job(job_id, result=None)
        redis_service.complete_job(job_id, result)
        
        # Update worker status back to idle
        redis_service.update_worker_status(worker_id, "idle")
        
        # Create completion message for client
        completion = JobCompletedMessage(
            job_id=job_id,
            result=result
        )
        
        # Send completion to subscribed client
        client_notified = await connection_manager.send_job_update(job_id, completion)
        
    # Worker handlers for push-based notification system
    async def handle_worker_heartbeat(worker_id: str, message: WorkerHeartbeatMessage):
        """Handle worker heartbeat and status update"""
        # Check if worker exists in Redis
        worker_exists = redis_service.worker_exists(worker_id)
        if not worker_exists:
            # Try to re-register the worker with minimal info
            worker_info = {
                "machine_id": "unknown",  # We don't have this info from heartbeat
                "gpu_id": "unknown"      # We don't have this info from heartbeat
            }
            redis_service.register_worker(worker_id, worker_info)
        
        # Update worker heartbeat and status in Redis
        redis_service.update_worker_heartbeat(worker_id, message.status)
            
    # handle_legacy_heartbeat function removed - legacy code
    # Workers should now use WORKER_HEARTBEAT message type

    async def handle_worker_status(worker_id: str, message: WorkerStatusMessage):
        """Handle worker status update messages"""
        # Check if worker exists in Redis
        worker_exists = redis_service.worker_exists(worker_id)
        if not worker_exists:
            # Try to register the worker with minimal info
            worker_info = {
                "machine_id": "unknown",
                "gpu_id": "unknown"
            }
            redis_service.register_worker(worker_id, worker_info)
        
        # Update worker status in Redis
        redis_service.update_worker_status(worker_id, message.status)
        
        # Update worker status in connection manager
        await connection_manager.update_worker_status(worker_id, message.status)
    
    async def handle_claim_job(worker_id: str, message: ClaimJobMessage):
        """Handle job claim request from a worker"""
        job_id = message.job_id
        claim_timeout = message.claim_timeout
        
        # Try to claim the job
        job_data = redis_service.claim_job(job_id, worker_id, claim_timeout)
        
        # Prepare response
        if job_data:
            # Update worker status to busy
            await connection_manager.update_worker_status(worker_id, "busy")
            redis_service.update_worker_status(worker_id, "busy")
            
            # Send successful claim response
            response = JobClaimedMessage(
                job_id=job_id,
                worker_id=worker_id,
                success=True,
                job_data=job_data
            )
        else:
            # Send failed claim response
            response = JobClaimedMessage(
                job_id=job_id,
                worker_id=worker_id,
                success=False,
                message="Job not available or already claimed"
            )
        
        await connection_manager.send_to_worker(worker_id, response)
        
        # If claim was successful, publish job update
        if job_data:
            redis_service.publish_job_update(job_id, "claimed", worker_id=worker_id)
            
            # Notify client about job status change
            if job_id in connection_manager.job_subscriptions:
                client_id = connection_manager.job_subscriptions[job_id]
                update = JobUpdateMessage(
                    job_id=job_id,
                    status="claimed",
                    worker_id=worker_id
                )
                await connection_manager.send_to_client(client_id, update)
    
    # handle_subscribe_job_notifications function removed - redundant functionality
    # Workers now automatically receive job notifications when idle

# Redis Pub/Sub listener for job updates and notifications
async def start_redis_listener():
    """Start listening for Redis pub/sub messages"""
    print("\n\n==========================================")
    print("=== 🔄 STARTING REDIS PUB/SUB LISTENER ====")
    print("==========================================\n\n")
    logger.info("Starting Redis pub/sub listener")
    
    # Connect to Redis async client
    try:
        await redis_service.connect_async()
        logger.info("Successfully connected to Redis async client")
    except Exception as e:
        logger.error(f"Failed to connect to Redis async client: {str(e)}")
        print("\n\n====================================================")
        print("=== ⚠️ REDIS CONNECTION FAILED ===")
        print(f"=== ERROR: {str(e)} ===")
        print("====================================================\n\n")
        return
        
    # Define job update message handler
    async def handle_job_update(message):
        try:
            # Parse message data
            data = json.loads(message["data"])
            job_id = data.get("job_id")
            
            if job_id:
                # Check for subscribed client
                await connection_manager.send_job_update(job_id, data)
        except Exception as e:
            logger.error(f"Error handling Redis job update message: {str(e)}")
    
    # Define job notification message handler
    async def handle_job_notification(message):
        try:
            # Log the raw message
            print("\n\n==========================================================")
            print("=== PUB/SUB JOB NOTIFICATION MESSAGE RECEIVED ===")
            print(f"=== CHANNEL: {message.get('channel')} ===")
            print(f"=== DATA: {message.get('data')} ===")
            print("==========================================================\n\n")
            
            # Parse message data
            data = json.loads(message["data"])
            message_type = data.get("type")
            logger.debug(f"Processing notification message of type: {message_type}")
            
            if message_type == "job_available":
                job_id = data.get("job_id")
                job_type = data.get("job_type")
                
                if job_id and job_type:
                    # Log job details
                    logger.info(f"New job available: ID={job_id}, Type={job_type}")
                    
                    # Create job available notification message
                    notification = JobAvailableMessage(
                        job_id=job_id,
                        job_type=job_type,
                        job_request_payload=data.get("job_request_payload")
                    )
                    
                    # Send notification to idle workers
                    worker_count = await connection_manager.notify_idle_workers(notification)
                    logger.info(f"Notified {worker_count} idle workers about job {job_id}")
            
        except Exception as e:
            logger.error(f"Error handling Redis job notification message: {str(e)}")
            print("\n\n===================================================")
            print("=== ⚠️ ERROR PROCESSING JOB NOTIFICATION ===")
            print(f"=== ERROR: {str(e)} ===")
            print("===================================================\n\n")
    
    # Subscribe to channels
    try:
        await redis_service.subscribe_to_channel("job_updates", handle_job_update)
        await redis_service.subscribe_to_channel("job_notifications", handle_job_notification)
    except Exception as e:
        pass

# Function to listen for Redis pub/sub messages
async def listen_for_messages(pubsub):
    """Listen for messages on Redis pub/sub"""
    message_counter = 0
    
    while True:
        try:
            # Get message
            message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1)
            
            if not message:
                await asyncio.sleep(0.01)  # Short sleep to prevent CPU spinning
                continue
                
            message_counter += 1
            
            # Process message based on channel
            try:
                channel = message['channel'].decode('utf-8')
                data = message['data'].decode('utf-8')
                
                # Process the message
                
                if channel == "job_updates":
                    await process_job_update(json.loads(data))
                elif channel == "worker_updates":
                    await process_worker_update(json.loads(data))
                elif channel == "job_available":
                    await process_job_available(json.loads(data))
                else:
                    pass
            except json.JSONDecodeError:
                pass
            except Exception as e:
                pass
                
        except Exception as e:
            # Implement exponential backoff for reconnection attempts
            retry_delay = 1
            max_retry_delay = 30
            for attempt in range(1, 6):  # 5 retry attempts
                await asyncio.sleep(retry_delay)
                try:
                    # Try to reconnect to Redis
                    await redis_service.connect_async()  # This will reconnect the async client
                    # If we get here, connection was successful
                    break
                except Exception:
                    retry_delay = min(retry_delay * 2, max_retry_delay)  # Exponential backoff with a cap

# Helper functions for message processing
async def process_job_update(data):
    """Process a job update message"""
    job_id = data.get("job_id")
    status = data.get("status")
    worker_id = data.get("worker_id")
    
    if not job_id or not status:
        return
    
    # Forward update to subscribed clients
    client_count = await connection_manager.send_job_update(job_id, data)
    
    # Also broadcast to monitors with job_updates channel in their filters
    monitor_message = {
        "type": "job_update",
        "job_id": job_id,
        "status": status,
        "worker_id": worker_id,
        "timestamp": time.time(),
        "data": data
    }
    
    await connection_manager.broadcast_to_monitors(monitor_message)

async def process_worker_update(data):
    """Process a worker update message"""
    worker_id = data.get("worker_id")
    status = data.get("status")
    
    if not worker_id:
        return
    
    # Update local worker status tracking
    if status:
        await connection_manager.update_worker_status(worker_id, status)
        
    # Broadcast worker update to all monitors
    monitor_message = {
        "type": "worker_update",
        "worker_id": worker_id,
        "status": status,
        "timestamp": time.time(),
        "data": data
    }
    
    await connection_manager.broadcast_to_monitors(monitor_message)

async def process_job_available(data):
    """Process a job available message"""
    job_id = data.get("job_id")
    job_type = data.get("job_type")
    
    if not job_id or not job_type:
        return
    
    # Create notification message
    notification = JobAvailableMessage(
        job_id=job_id,
        job_type=job_type,
        job_request_payload=data.get("job_request_payload")
    )
    
    # Notify idle workers
    worker_count = await connection_manager.notify_idle_workers(notification)
    
    # Also broadcast to monitors with job_notifications channel in their filters
    monitor_message = {
        "type": "job_available",
        "job_id": job_id,
        "job_type": job_type,
        "job_request_payload": data.get("job_request_payload"),
        "timestamp": time.time(),
        "data": data
    }
    
    await connection_manager.broadcast_to_monitors(monitor_message)
