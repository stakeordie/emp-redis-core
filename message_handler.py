#!/usr/bin/env python3
# Implementation of the MessageHandlerInterface
import asyncio
import json
import time
import uuid
from typing import Dict, Any, Optional, Callable, Awaitable, List
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from websockets.exceptions import ConnectionClosedError

from .interfaces.message_handler_interface import MessageHandlerInterface
from .redis_service import RedisService
from .connection_manager import ConnectionManager
from .message_router import MessageRouter
from .message_models import (
    MessageModels, MessageType, BaseMessage, ErrorMessage, JobAcceptedMessage, 
    JobStatusMessage, JobUpdateMessage, WorkerRegisteredMessage, JobAvailableMessage,
    JobCompletedMessage, SubmitJobMessage, GetJobStatusMessage, RegisterWorkerMessage, 
    UpdateJobProgressMessage, CompleteJobMessage, WorkerHeartbeatMessage, WorkerStatusMessage, 
    ClaimJobMessage, JobClaimedMessage, SubscribeJobMessage, SubscribeStatsMessage, 
    GetStatsMessage, ConnectionEstablishedMessage, JobAssignedMessage
)
from .utils.logger import logger

class MessageHandler(MessageHandlerInterface):
    """
    Implementation of the MessageHandlerInterface.
    
    This class provides a concrete implementation of the message handling
    contract defined in the interface, organizing and maintaining
    message handling logic for the application.
    """
    
    def __init__(self, redis_service: RedisService, connection_manager: ConnectionManager,
                 message_router: MessageRouter, message_models: MessageModels):
        """
        Initialize the message handler with required services.
        
        Args:
            redis_service: Redis service instance
            connection_manager: Connection manager instance
            message_router: Message router instance
            message_models: Message models instance
        """
        self.redis_service = redis_service
        self.connection_manager = connection_manager
        self.message_router = message_router
        self.message_models = message_models
        self._background_tasks: List[asyncio.Task] = []
    
    def init_routes(self, app: FastAPI) -> None:
        """
        Initialize routes for the FastAPI application.
        
        Args:
            app: FastAPI application instance
        """
        # Register WebSocket routes
        
        # Client WebSocket route
        @app.websocket("/ws/client/{client_id}")
        async def client_websocket_route(websocket: WebSocket, client_id: str):
            await self.client_websocket(websocket, client_id)
        
        # Worker WebSocket route
        @app.websocket("/ws/worker/{worker_id}")
        async def worker_websocket_route(websocket: WebSocket, worker_id: str):
            await self.worker_websocket(websocket, worker_id)
        
        # Monitor WebSocket route
        @app.websocket("/ws/monitor/{monitor_id}")
        async def monitor_websocket_route(websocket: WebSocket, monitor_id: str):
            await self.monitor_websocket(websocket, monitor_id)
        
        logger.info("Initialized WebSocket routes")
    
    async def client_websocket(self, websocket: WebSocket, client_id: str) -> None:
        """
        Handle client WebSocket connections.
        
        Args:
            websocket: WebSocket connection
            client_id: Client identifier
        """
        # Accept the connection
        try:
            await websocket.accept()
            
            # Store the connection
            self.connection_manager.client_connections[client_id] = websocket
            
            # Send an immediate welcome message
            welcome_message = {"type": "connection_established", "message": f"Welcome Client {client_id}! Connected to Redis Hub"}
            await websocket.send_text(json.dumps(welcome_message))
            
            logger.info(f"Client connected: {client_id}")
        except Exception as e:
            logger.error(f"Error accepting client connection {client_id}: {str(e)}")
            return
        
        try:
            while True:
                # Receive message from client
                message_text = await websocket.receive_text()
                
                # Parse message
                try:
                    message_data = json.loads(message_text)
                    message_type = message_data.get("type")
                    
                    # Process message based on type
                    if message_type == "submit_job":
                        await self.handle_submit_job(client_id, message_data)
                    elif message_type == "get_job_status":
                        job_id = message_data.get("job_id")
                        if job_id:
                            await self.handle_get_job_status(client_id, job_id)
                        else:
                            error_message = {"type": "error", "error": "Missing job_id in get_job_status request"}
                            await self.connection_manager.send_to_client(client_id, error_message)
                    elif message_type == "subscribe_job":
                        job_id = message_data.get("job_id")
                        if job_id:
                            await self.connection_manager.subscribe_to_job(job_id, client_id)
                            await websocket.send_text(json.dumps({"type": "subscription_confirmed", "job_id": job_id}))
                        else:
                            error_message = {"type": "error", "error": "Missing job_id in subscribe_job request"}
                            await self.connection_manager.send_to_client(client_id, error_message)
                    elif message_type == "subscribe_stats":
                        self.handle_subscribe_stats(client_id)
                        await websocket.send_text(json.dumps({"type": "subscription_confirmed", "subscription": "stats"}))
                    elif message_type == "get_stats":
                        await self.handle_get_stats(client_id)
                    elif message_type == "stay_alive":
                        # Respond to stay_alive to keep connection alive
                        await websocket.send_text(json.dumps({"type": "stay_alive_response", "timestamp": time.time()}))
                    else:
                        # Handle unrecognized message type
                        error_message = {"type": "error", "error": f"Unsupported message type: {message_type}"}
                        await self.connection_manager.send_to_client(client_id, error_message)
                        
                except json.JSONDecodeError:
                    # Handle invalid JSON
                    error_message = {"type": "error", "error": "Invalid JSON format"}
                    await self.connection_manager.send_to_client(client_id, error_message)
                    
                except Exception as e:
                    # Handle processing error
                    error_message = {"type": "error", "error": "Server error processing message"}
                    await self.connection_manager.send_to_client(client_id, error_message)
                
        except WebSocketDisconnect:
            self.connection_manager.disconnect_client(client_id)
            logger.info(f"Client disconnected: {client_id}")
        
        except Exception as e:
            # Handle unexpected error
            self.connection_manager.disconnect_client(client_id)
            logger.error(f"Unexpected error in client connection {client_id}: {str(e)}")
    
    async def worker_websocket(self, websocket: WebSocket, worker_id: str) -> None:
        """
        Handle worker WebSocket connections.
        
        Args:
            websocket: WebSocket connection
            worker_id: Worker identifier
        """
        # Accept the connection
        try:
            # Accept the connection
            await websocket.accept()
            
            # Store the connection
            self.connection_manager.worker_connections[worker_id] = websocket
            self.connection_manager.worker_status[worker_id] = "idle"  # Set initial status to idle
            
            # Send an immediate welcome message
            welcome_message = {"type": "connection_established", "message": f"Welcome Worker {worker_id}! Connected to Redis Hub"}
            logger.debug(f"[WORKER-CONN] Sending welcome message to worker {worker_id}")
            await websocket.send_text(json.dumps(welcome_message))
            
            logger.info(f"Worker connected: {worker_id}")
        except WebSocketDisconnect as e:
            logger.error(f"WebSocket disconnected during worker connection setup for {worker_id}: {str(e)}")
            return
        except ConnectionClosedError as e:
            logger.error(f"Connection closed during worker connection setup for {worker_id}: {str(e)}")
            return
        except Exception as e:
            logger.error(f"Error accepting worker connection {worker_id}: {str(e)}")
            return
        
        try:
            while True:
                # Receive message from worker
                message_text = await websocket.receive_text()
                
                # Parse message
                try:
                    message_data = json.loads(message_text)
                    logger.debug(f"Received message from worker {worker_id}: {message_data}")
                    
                    # Process message based on type
                    await self.handle_worker_message(websocket, message_data, worker_id)
                        
                except json.JSONDecodeError:
                    # Handle invalid JSON
                    error_message = {"type": "error", "error": "Invalid JSON format"}
                    await self.connection_manager.send_to_worker(worker_id, error_message)
                    
                except Exception as e:
                    # Handle processing error
                    error_message = {"type": "error", "error": "Server error processing message"}
                    await self.connection_manager.send_to_worker(worker_id, error_message)
                
        except WebSocketDisconnect:
            self.connection_manager.disconnect_worker(worker_id)
            # Mark worker as disconnected in Redis
            self.redis_service.update_worker_status(worker_id, "disconnected")
            logger.info(f"Worker disconnected: {worker_id}")
        
        except Exception as e:
            # Handle unexpected error
            self.connection_manager.disconnect_worker(worker_id)
            # Mark worker as disconnected in Redis
            self.redis_service.update_worker_status(worker_id, "disconnected")
            logger.error(f"Unexpected error in worker connection {worker_id}: {str(e)}")
    
    async def monitor_websocket(self, websocket: WebSocket, monitor_id: str) -> None:
        """
        Handle monitor WebSocket connections.
        
        Args:
            websocket: WebSocket connection
            monitor_id: Monitor identifier
        """
        # Accept the connection
        try:
            await websocket.accept()
            
            # Store the connection
            self.connection_manager.monitor_connections[monitor_id] = websocket
            
            # Send an immediate welcome message
            welcome_message = {"type": "connection_established", "message": f"Welcome Monitor {monitor_id}! Connected to Redis Hub"}
            await websocket.send_text(json.dumps(welcome_message))
            
            # Send immediate system status update
            await self.connection_manager.send_system_status_to_monitors(self.redis_service)
            
            logger.info(f"Monitor connected: {monitor_id}")
        except Exception as e:
            logger.error(f"Error accepting monitor connection {monitor_id}: {str(e)}")
            return
        
        try:
            while True:
                # Receive message from monitor
                message_text = await websocket.receive_text()
                
                # Parse message
                try:
                    message_data = json.loads(message_text)
                    message_type = message_data.get("type")
                    
                    # Process message based on type
                    if message_type == "subscribe":
                        channels = message_data.get("channels", [])
                        self.connection_manager.set_monitor_subscriptions(monitor_id, channels)
                        await websocket.send_text(json.dumps({
                            "type": "subscription_confirmed",
                            "channels": channels
                        }))
                    elif message_type == "get_system_status":
                        # Send immediate system status update using the enhanced method
                        await self.connection_manager.send_system_status_to_monitors(self.redis_service)
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
            self.connection_manager.disconnect_monitor(monitor_id)
            logger.info(f"Monitor disconnected: {monitor_id}")
        
        except Exception as e:
            # Handle unexpected error
            self.connection_manager.disconnect_monitor(monitor_id)
            logger.error(f"Unexpected error in monitor connection {monitor_id}: {str(e)}")
    
    async def start_background_tasks(self) -> None:
        """
        Start background tasks for the application.
        
        This method initializes and starts any background tasks
        needed for the application, such as periodic stats broadcasts,
        cleanup tasks, etc.
        """
        # Start the background tasks
        self._background_tasks.append(asyncio.create_task(self._broadcast_stats_task()))
        self._background_tasks.append(asyncio.create_task(self._cleanup_stale_claims_task()))
        self._background_tasks.append(asyncio.create_task(self._mark_stale_workers_task()))
        self._background_tasks.append(asyncio.create_task(self._monitor_status_update_task()))
        self._background_tasks.append(asyncio.create_task(self._start_redis_listener()))
        
        logger.info("Started all background tasks")
    
    async def stop_background_tasks(self) -> None:
        """
        Stop background tasks for the application.
        
        This method properly stops and cleans up any background
        tasks started by the application.
        """
        # Cancel all background tasks
        for task in self._background_tasks:
            if not task.done():
                task.cancel()
                
        # Wait for all tasks to complete their cancellation
        if self._background_tasks:
            await asyncio.gather(*self._background_tasks, return_exceptions=True)
            
        # Clear the task list
        self._background_tasks.clear()
        
        logger.info("Stopped all background tasks")
    
    async def handle_client_message(self, websocket: WebSocket, 
                                  message: Dict[str, Any], 
                                  client_id: str) -> None:
        """
        Handle a message from a client.
        
        Args:
            websocket: WebSocket connection
            message: Message data
            client_id: Client identifier
        """
        # Extract message type
        message_type = message.get("type")
        
        # Process message based on type
        if message_type == "submit_job":
            await self.handle_submit_job(client_id, message)
        elif message_type == "get_job_status":
            job_id = message.get("job_id")
            if job_id:
                await self.handle_get_job_status(client_id, job_id)
            else:
                error_message = {"type": "error", "error": "Missing job_id in get_job_status request"}
                await self.connection_manager.send_to_client(client_id, error_message)
        elif message_type == "subscribe_job":
            job_id = message.get("job_id")
            if job_id:
                await self.connection_manager.subscribe_to_job(job_id, client_id)
                await websocket.send_text(json.dumps({"type": "subscription_confirmed", "job_id": job_id}))
            else:
                error_message = {"type": "error", "error": "Missing job_id in subscribe_job request"}
                await self.connection_manager.send_to_client(client_id, error_message)
        elif message_type == "subscribe_stats":
            self.handle_subscribe_stats(client_id)
            await websocket.send_text(json.dumps({"type": "subscription_confirmed", "subscription": "stats"}))
        elif message_type == "get_stats":
            await self.handle_get_stats(client_id)
        elif message_type == "stay_alive":
            # Respond to stay_alive to keep connection alive
            await websocket.send_text(json.dumps({"type": "stay_alive_response", "timestamp": time.time()}))
        else:
            # Handle unrecognized message type
            error_message = {"type": "error", "error": f"Unsupported message type: {message_type}"}
            await self.connection_manager.send_to_client(client_id, error_message)
            
    async def handle_submit_job(self, client_id: str, message_data: Dict[str, Any]) -> None:
        """
        Handle job submission from a client.
        
        Args:
            client_id: Client identifier
            message_data: Job submission message data
        """
        # Generate job ID if not provided
        job_id = f"job-{uuid.uuid4()}"
        
        # Extract data directly from the dictionary
        job_type = message_data.get('job_type', 'unknown')
        priority = message_data.get('priority', 0)
        payload = message_data.get('payload', {})
        
        # Add job to Redis
        job_data = self.redis_service.add_job(
            job_id=job_id,
            job_type=job_type,
            priority=priority,
            job_request_payload=payload,
            client_id=client_id
        )
        
        # Create job available notification for workers
        notification = JobAvailableMessage(
            job_id=job_id,
            job_type=job_type,
            priority=priority,
            job_request_payload=payload
        )
        
        # Send notification to idle workers and get count
        worker_count = await self.connection_manager.notify_idle_workers(notification)
        
        # Send confirmation response with worker count
        response = JobAcceptedMessage(
            job_id=job_id,
            position=job_data.get("position", -1),
            notified_workers=worker_count  # Include the count of notified workers
        )
        
        await self.connection_manager.send_to_client(client_id, response)
        # Automatically subscribe client to job updates
        await self.connection_manager.subscribe_to_job(client_id, job_id)
    
    async def handle_get_job_status(self, client_id: str, job_id: str) -> None:
        """
        Handle job status request from a client.
        
        Args:
            client_id: Client identifier
            job_id: Job identifier
        """
        # Get job status from Redis
        job_data = self.redis_service.get_job_status(job_id)
        
        if not job_data:
            error_message = ErrorMessage(error=f"Job {job_id} not found")
            await self.connection_manager.send_to_client(client_id, error_message)
            return
        
        # Create response
        response = JobStatusMessage(
            job_id=job_id,
            status=job_data.get("status", "unknown"),
            progress=int(job_data.get("progress", 0)) if "progress" in job_data else None,
            worker_id=job_data.get("worker"),
            started_at=float(job_data["started_at"]) if "started_at" in job_data else None,
            completed_at=float(job_data["completed_at"]) if "completed_at" in job_data else None,
            result=job_data.get("result"),
            message=job_data.get("message")
        )
        await self.connection_manager.send_to_client(client_id, response)
        
        # Subscribe client to future updates for this job
        await self.connection_manager.subscribe_to_job(job_id, client_id)
    
    
    def handle_subscribe_stats(self, client_id: str) -> None:
        """
        Handle stats subscription request from a client.
        
        Args:
            client_id: Client identifier
        """
        self.connection_manager.subscribe_to_stats(client_id)
    
    async def handle_get_stats(self, client_id: str) -> None:
        """
        Handle stats request from a client.
        
        Args:
            client_id: Client identifier
        """
        stats = self.redis_service.get_stats()
        
        # Format the stats as a proper message with a type field
        stats_response = {
            "type": MessageType.STATS_RESPONSE,
            "stats": stats,
            "timestamp": time.time()
        }
        
        await self.connection_manager.send_to_client(client_id, stats_response)
    
    async def handle_worker_message(self, websocket: WebSocket, 
                                  message: Dict[str, Any], 
                                  worker_id: str) -> None:
        """
        Handle a message from a worker.
        
        Args:
            websocket: WebSocket connection
            message: Message data
            worker_id: Worker identifier
        """
        # Parse message using the models
        message_obj = self.message_models.parse_message(message)
        
        if not message_obj:
            logger.error(f"Invalid message received from worker {worker_id}")
            error_message = {"type": "error", "error": "Invalid message format"}
            await self.connection_manager.send_to_worker(worker_id, error_message)
            return
            
        # Process message based on type
        if message_obj.type == MessageType.REGISTER_WORKER:
            # Cast message to the expected type for type checking
            register_message = RegisterWorkerMessage(**message_obj.dict())
            await self.handle_register_worker(worker_id, register_message)
        elif message_obj.type == MessageType.UPDATE_JOB_PROGRESS:
            # Cast message to the expected type for type checking
            # Create a properly typed message object
            progress_message = UpdateJobProgressMessage(**message_obj.dict())
            await self.handle_update_job_progress(worker_id, progress_message)
        elif message_obj.type == MessageType.COMPLETE_JOB:
            # Cast message to the expected type for type checking
            # Create a properly typed message object
            complete_message = CompleteJobMessage(**message_obj.dict())
            await self.handle_complete_job(worker_id, complete_message)
        elif message_obj.type == MessageType.WORKER_HEARTBEAT:
            # Cast message to the expected type for type checking
            # Create a properly typed message object
            heartbeat_message = WorkerHeartbeatMessage(**message_obj.dict())
            await self.handle_worker_heartbeat(worker_id, heartbeat_message)
        elif message_obj.type == MessageType.WORKER_STATUS:
            # Cast message to the expected type for type checking
            # Create a properly typed message object
            status_message = WorkerStatusMessage(**message_obj.dict())
            await self.handle_worker_status(worker_id, status_message)
        elif message_obj.type == MessageType.CLAIM_JOB:
            # Cast message to the expected type for type checking
            # Create a properly typed message object
            claim_message = ClaimJobMessage(**message_obj.dict())
            await self.handle_claim_job(worker_id, claim_message)
        else:
            # Handle unrecognized message type
            error_message = {"type": "error", "error": f"Unsupported message type: {message_obj.type}"}
            await self.connection_manager.send_to_worker(worker_id, error_message)
            
    async def handle_register_worker(self, worker_id: str, message: 'RegisterWorkerMessage') -> None:
        """
        Handle worker registration.
        
        Args:
            worker_id: Worker identifier
            message: Register worker message
        """
        # Update worker info in Redis
        # Create a capabilities dict from machine_id and gpu_id
        capabilities = {
            "machine_id": message.machine_id,
            "gpu_id": message.gpu_id
        }
        
        self.redis_service.register_worker(
            worker_id=worker_id,
            capabilities=capabilities
        )
        
        # Send confirmation
        response = WorkerRegisteredMessage(worker_id=worker_id)
        await self.connection_manager.send_to_worker(worker_id, response)
        
    
    async def handle_update_job_progress(self, worker_id: str, message: 'UpdateJobProgressMessage') -> None:
        """
        Handle job progress update from a worker.
        
        Args:
            worker_id: Worker identifier
            message: Update job progress message
        """
        # Update job progress in Redis
        self.redis_service.update_job_progress(
            job_id=message.job_id,
            progress=message.progress,
            message=message.message
        )
        
        # Forward progress update to subscribed clients
        await self.connection_manager.broadcast_job_notification(message.dict())
        
        logger.info(f"Job progress updated: {message.job_id}, progress: {message.progress}%")
    
    async def handle_complete_job(self, worker_id: str, message: 'CompleteJobMessage') -> None:
        """
        Handle job completion from a worker.
        
        Args:
            worker_id: Worker identifier
            message: Complete job message
        """
        # Update job status in Redis
        self.redis_service.complete_job(
            job_id=message.job_id,
            result=message.result
        )
        
        # Mark worker as idle
        self.redis_service.update_worker_status(worker_id, "idle")
        
        # Forward completion update to subscribed clients
        await self.connection_manager.broadcast_job_notification(message.dict())
        
        logger.info(f"Job completed: {message.job_id} by worker: {worker_id}")
    
    async def handle_worker_heartbeat(self, worker_id: str, message: 'WorkerHeartbeatMessage') -> None:
        """
        Handle worker heartbeat.
        
        Args:
            worker_id: Worker identifier
            message: Worker heartbeat message
        """
        # Update worker last seen timestamp in Redis
        self.redis_service.update_worker_heartbeat(worker_id)
        
        # Optionally update worker status if provided
        if hasattr(message, 'status') and message.status:
            self.redis_service.update_worker_status(worker_id, message.status)
        
        # No response needed for heartbeats
        logger.debug(f"Worker heartbeat received: {worker_id}")
    
    async def handle_worker_status(self, worker_id: str, message: 'WorkerStatusMessage') -> None:
        """
        Handle worker status update.
        
        Args:
            worker_id: Worker identifier
            message: Worker status message
        """
        # Update worker status in Redis
        # Use 'idle' as default status if message.status is None
        status = message.status if message.status is not None else "idle"
        self.redis_service.update_worker_status(worker_id, status)
        
        # If worker is now idle, notify about available jobs
        if status == "idle":
            # TODO: Replace with appropriate method to notify worker about available jobs
            # await self.connection_manager.notify_worker_about_jobs(worker_id)
            # For now, we'll just log this event
            logger.info(f"Worker {worker_id} is now idle and ready for jobs")
        
        logger.info(f"Worker status updated: {worker_id}, status: {message.status}")
    
    async def handle_claim_job(self, worker_id: str, message: 'ClaimJobMessage') -> None:
        """
        Handle job claim from a worker.
        
        Args:
            worker_id: Worker identifier
            message: Claim job message
        """
        # Attempt to claim the job in Redis
        success = self.redis_service.claim_job(message.job_id, worker_id)
        
        if success:
            # Update worker status to working
            self.redis_service.update_worker_status(worker_id, "working")
            
            # Get job details
            job_data = self.redis_service.get_job_status(message.job_id)
            
            if job_data:
                # Send job details to worker
                job_details = JobAssignedMessage(
                    job_id=message.job_id,
                    job_type=job_data.get("job_type", "unknown"),
                    priority=job_data.get("priority", 0),
                    params=job_data.get("job_request_payload", {})
                )
                await self.connection_manager.send_to_worker(worker_id, job_details)
                
                # Notify clients that job is now processing
                status_update = JobStatusMessage(
                    job_id=message.job_id,
                    status="processing",
                    worker_id=worker_id
                )
                await self.connection_manager.broadcast_job_notification(status_update.dict())
                
                logger.info(f"Job claimed: {message.job_id} by worker: {worker_id}")
            else:
                logger.error(f"Job claimed but details not found: {message.job_id}")
        else:
            # Job could not be claimed
            error_message = ErrorMessage(error=f"Failed to claim job {message.job_id}")
            await self.connection_manager.send_to_worker(worker_id, error_message)
            
            logger.warning(f"Failed job claim: {message.job_id} by worker: {worker_id}")
    
    async def handle_monitor_message(self, websocket: WebSocket, 
                                   message: Dict[str, Any], 
                                   monitor_id: str) -> None:
        """
        Handle a message from a monitor.
        
        Args:
            websocket: WebSocket connection
            message: Message data
            monitor_id: Monitor identifier
        """
        # Extract message type
        message_type = message.get("type")
        
        # Process message based on type
        if message_type == "subscribe":
            channels = message.get("channels", [])
            self.connection_manager.set_monitor_subscriptions(monitor_id, channels)
            await websocket.send_text(json.dumps({
                "type": "subscription_confirmed",
                "channels": channels
            }))
        elif message_type == "get_system_status":
            # Send immediate system status update using the enhanced method
            await self.connection_manager.send_system_status_to_monitors(self.redis_service)
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
        
    # Background task methods
    async def _broadcast_stats_task(self) -> None:
        """
        Periodically fetch stats from Redis and broadcast to subscribed clients
        """
        last_stats = None  # Track previous stats to detect changes
        
        while True:
            try:
                # Check if there are any clients subscribed or monitors connected
                # Ensure we're checking a sized collection, not an int
                stats_subscriptions = self.connection_manager.stats_subscriptions
                monitor_connections = self.connection_manager.monitor_connections
                subscribers_count = len(stats_subscriptions) if hasattr(stats_subscriptions, '__len__') else 0
                monitors_count = len(monitor_connections) if hasattr(monitor_connections, '__len__') else 0
                
                # Skip if no clients are subscribed AND no monitors are connected
                if subscribers_count == 0 and monitors_count == 0:
                    await asyncio.sleep(3)  # Check less frequently if no subscribers
                    continue
                    
                # Get stats from Redis
                current_stats = self.redis_service.get_stats()
                
                # Skip broadcast if stats haven't changed
                if current_stats == last_stats:
                    await asyncio.sleep(1)  # Check frequently but don't broadcast
                    continue
                    
                # Update last_stats for next comparison
                last_stats = current_stats
                
                # Format the stats as a proper message with a type field
                stats_response = {
                    "type": "stats_response",
                    "stats": current_stats,
                    "timestamp": time.time()
                }
                
                # Broadcast to all subscribed clients
                await self.connection_manager.broadcast_stats(stats_response)
                
                # Also send to all connected monitors
                for monitor_id in self.connection_manager.monitor_connections:
                    await self.connection_manager.send_to_monitor(monitor_id, stats_response)
                
            except Exception as e:
                logger.error(f"Error in stats broadcast task: {str(e)}")
                
            # Sleep before next update
            await asyncio.sleep(1)
    
    async def _cleanup_stale_claims_task(self) -> None:
        """
        Periodically check for and clean up stale job claims
        """
        while True:
            try:
                # Perform cleanup
                cleaned_jobs = self.redis_service.cleanup_stale_claims()
                
                # Ensure cleaned_jobs is a dict before using len() and items()
                if cleaned_jobs and isinstance(cleaned_jobs, dict) and len(cleaned_jobs) > 0:
                    logger.info(f"Cleaned up {len(cleaned_jobs)} stale job claims")
                    
                    # For each cleaned job, notify idle workers about it
                    for job_id, job_data in cleaned_jobs.items():
                        # Create job available notification
                        notification = {
                            "type": "job_available",
                            "job_id": job_id,
                            "job_type": job_data.get("job_type", "unknown"),
                            "priority": int(job_data.get("priority", 0)),
                            "job_request_payload": job_data.get("job_request_payload", {})
                        }
                        
                        # Send notification to idle workers
                        await self.connection_manager.notify_idle_workers(notification)
                        
            except Exception as e:
                logger.error(f"Error in stale claims cleanup task: {str(e)}")
                
            # Sleep before next cleanup
            await asyncio.sleep(30)  # Check every 30 seconds
    
    async def _mark_stale_workers_task(self) -> None:
        """
        Periodically check for and mark stale workers as disconnected
        """
        while True:
            try:
                # Get stale workers and mark them as disconnected
                # Check if the method exists, otherwise use a fallback approach
                if hasattr(self.redis_service, 'mark_stale_workers'):
                    stale_workers = self.redis_service.mark_stale_workers()
                else:
                    # Fallback: Get stale workers and update their status individually
                    stale_workers = self.redis_service.get_stale_workers() if hasattr(self.redis_service, 'get_stale_workers') else []
                    for worker_id in stale_workers:
                        self.redis_service.update_worker_status(worker_id, "disconnected")
                
                if stale_workers and len(stale_workers) > 0:
                    logger.info(f"Marked {len(stale_workers)} workers as disconnected due to inactivity")
                    
                    # Update connection manager with disconnected workers
                    for worker_id in stale_workers:
                        self.connection_manager.disconnect_worker(worker_id)
                        
            except Exception as e:
                logger.error(f"Error in stale workers cleanup task: {str(e)}")
                
            # Sleep before next cleanup
            await asyncio.sleep(60)  # Check every minute
    
    async def _monitor_status_update_task(self) -> None:
        """
        Periodically send system status updates to connected monitors
        """
        while True:
            try:
                # Check if there are any monitors connected
                monitors_count = len(self.connection_manager.monitor_connections)
                
                # Skip if no monitors are connected
                if monitors_count == 0:
                    await asyncio.sleep(5)  # Check less frequently if no monitors
                    continue
                    
                # Send system status to all connected monitors
                await self.connection_manager.send_system_status_to_monitors(self.redis_service)
                
            except Exception as e:
                logger.error(f"Error in monitor status update task: {str(e)}")
                
            # Sleep before next update
            await asyncio.sleep(5)  # Update every 5 seconds
    
    async def _start_redis_listener(self) -> None:
        """
        Start listening for Redis pub/sub messages
        """
        logger.info("Starting Redis pub/sub listener")
        
        # Connect to Redis async client
        try:
            await self.redis_service.connect_async()
            logger.info("Successfully connected to Redis async client")
        except Exception as e:
            logger.error(f"Failed to connect to Redis async client: {str(e)}")
            return
            
        # Define job update message handler
        async def handle_job_update(message):
            try:
                # Parse message data
                data = json.loads(message["data"])
                job_id = data.get("job_id")
                
                if job_id:
                    # Check for subscribed client
                    await self.connection_manager.send_job_update(job_id, data)
            except Exception as e:
                logger.error(f"Error handling Redis job update message: {str(e)}")
        
        # Define job notification message handler
        async def handle_job_notification(message):
            try:
                # Parse message data
                data = json.loads(message["data"])
                message_type = data.get("type")
                
                if message_type == "job_available":
                    job_id = data.get("job_id")
                    job_type = data.get("job_type")
                    
                    if job_id and job_type:
                        # Create job available notification message
                        notification = {
                            "type": "job_available",
                            "job_id": job_id,
                            "job_type": job_type,
                            "job_request_payload": data.get("job_request_payload")
                        }
                        
                        # Send notification to idle workers
                        worker_count = await self.connection_manager.notify_idle_workers(notification)
                        logger.info(f"Notified {worker_count} idle workers about job {job_id}")
                
            except Exception as e:
                logger.error(f"Error handling Redis job notification message: {str(e)}")
        
        # Subscribe to channels
        try:
            await self.redis_service.subscribe_to_channel("job_updates", handle_job_update)
            await self.redis_service.subscribe_to_channel("job_notifications", handle_job_notification)
        except Exception as e:
            logger.error(f"Error subscribing to Redis channels: {str(e)}")
    
    async def handle_subscribe_job(self, client_id: str, job_id: str) -> None:
        """
        Handle job subscription request from a client.
        
        Args:
            client_id: Client identifier
            job_id: Job identifier
        """
        await self.connection_manager.subscribe_to_job(job_id, client_id)
