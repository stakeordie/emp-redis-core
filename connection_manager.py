#!/usr/bin/env python3
# Core WebSocket connection manager for the queue system
import json
import asyncio
import time
from typing import Dict, Set, List, Any, Optional, Callable
from fastapi import WebSocket
from .models import BaseMessage, ErrorMessage
from .interfaces import ConnectionManagerInterface
from .utils.logger import logger

class ConnectionManager(ConnectionManagerInterface):
    """Manages WebSocket connections and message routing"""
    
    def __init__(self):
        """Initialize connection manager"""
        # Maps client IDs to their WebSocket connections
        self.client_connections: Dict[str, WebSocket] = {}
        
        # Maps worker IDs to their WebSocket connections
        self.worker_connections: Dict[str, WebSocket] = {}
        
        # Maps monitor IDs to their WebSocket connections
        self.monitor_connections: Dict[str, WebSocket] = {}
        
        # Maps job IDs to client IDs that should receive updates
        self.job_subscriptions: Dict[str, str] = {}
        
        # Set of client IDs subscribed to system stats updates
        self.stats_subscriptions: Set[str] = set()
        
        # Set of worker IDs subscribed to job notifications
        self.job_notification_subscriptions: Set[str] = set()
        
        # Set of monitor IDs with their message type filters
        self.monitor_filters: Dict[str, Set[str]] = {}
        
        # Tracks worker status ("idle", "busy", etc.)
        self.worker_status: Dict[str, str] = {}
        
        pass
    
    async def connect_client(self, websocket: WebSocket, client_id: str) -> None:
        """Connect a client WebSocket"""
        try:
            # Check if client is already connected to avoid duplicate connections
            if client_id in self.client_connections:
                try:
                    old_websocket = self.client_connections[client_id]
                    await self._close_websocket(old_websocket, f"Replaced by new connection for {client_id}")
                except Exception as e:
                    pass
            

            
            # We won't call websocket.accept() here since it's now handled in the router
            # This allows us to separate connection management from protocol handling
            self.client_connections[client_id] = websocket
            
            # Send a confirmation message to client
            try:
                welcome_message = {
                    "type": "connection_established",
                    "status": "connected", 
                    "client_id": client_id,
                    "message": "Successfully connected to Redis Hub"
                }
                await websocket.send_text(json.dumps(welcome_message))
            except Exception as e:
                pass
                
        except Exception as e:
            # Attempt to close connection gracefully if possible
            try:
                await websocket.close(code=1011, reason=f"Connection error: {str(e)}")
            except Exception:
                pass
            raise
    
    async def connect_worker(self, websocket: WebSocket, worker_id: str) -> None:
        """Connect a worker WebSocket"""
        try:
            # Check if worker is already connected to avoid duplicate connections
            if worker_id in self.worker_connections:
                try:
                    old_websocket = self.worker_connections[worker_id]
                    await self._close_websocket(old_websocket, f"Replaced by new connection for {worker_id}")
                except Exception:
                    pass
            

            
            # We won't call websocket.accept() here since it's now handled in the router
            # This allows us to separate connection management from protocol handling
            self.worker_connections[worker_id] = websocket
            self.worker_status[worker_id] = "idle"  # Set initial status to idle
            logger.info(f"Worker {worker_id} connected successfully")
            
            # Send a confirmation message to worker
            try:
                welcome_message = {
                    "type": "connection_established",
                    "status": "connected", 
                    "worker_id": worker_id,
                    "message": "Successfully connected to Redis Hub"
                }
                await websocket.send_text(json.dumps(welcome_message))
            except Exception:
                pass
                
        except Exception as e:
            # Attempt to close connection gracefully if possible
            try:
                await websocket.close(code=1011, reason=f"Connection error: {str(e)}")
            except Exception:
                pass
            raise
    
    def disconnect_client(self, client_id: str) -> None:
        """Disconnect a client"""
        if client_id in self.client_connections:
            # Store reference to WebSocket object before deletion
            websocket = self.client_connections[client_id]
            
            # Clean up connections dict first
            del self.client_connections[client_id]
            
            # Clean up subscriptions
            if client_id in self.stats_subscriptions:
                self.stats_subscriptions.remove(client_id)
                
            # Clean up job subscriptions
            jobs_to_remove = []
            for job_id, subscriber_id in self.job_subscriptions.items():
                if subscriber_id == client_id:
                    jobs_to_remove.append(job_id)
            
            for job_id in jobs_to_remove:
                del self.job_subscriptions[job_id]
            

            
            # Try to close the connection gracefully (async context but sync function)
            try:
                asyncio.create_task(self._close_websocket(websocket, f"Client {client_id} disconnected"))
            except Exception:
                pass
    
    def disconnect_worker(self, worker_id: str) -> None:
        """Disconnect a worker"""
        if worker_id in self.worker_connections:
            # Store reference to WebSocket object before deletion
            websocket = self.worker_connections[worker_id]
            
            # Clean up connections dict first
            del self.worker_connections[worker_id]
            
            # Clean up worker subscriptions
            if worker_id in self.job_notification_subscriptions:
                self.job_notification_subscriptions.remove(worker_id)
                
            # Clean up worker status
            if worker_id in self.worker_status:
                del self.worker_status[worker_id]
                

            
            # Try to close the connection gracefully (async context but sync function)
            try:
                asyncio.create_task(self._close_websocket(websocket, f"Worker {worker_id} disconnected"))
            except Exception:
                pass
                
    async def connect_monitor(self, websocket: WebSocket, monitor_id: str) -> None:
        """Connect a monitor WebSocket"""
        try:
            # Check if monitor is already connected to avoid duplicate connections
            if monitor_id in self.monitor_connections:
                try:
                    old_websocket = self.monitor_connections[monitor_id]
                    await self._close_websocket(old_websocket, f"Replaced by new connection for {monitor_id}")
                except Exception:
                    pass
            

            
            # We won't call websocket.accept() here since it's now handled in the router
            # This allows us to separate connection management from protocol handling
            self.monitor_connections[monitor_id] = websocket
            self.monitor_filters[monitor_id] = set()  # Initialize with empty filter set (receive all messages)
            
            # Send a confirmation message to monitor
            try:
                welcome_message = {
                    "type": "connection_established",
                    "status": "connected", 
                    "monitor_id": monitor_id,
                    "message": "Successfully connected to Redis Hub Monitor API"
                }
                await websocket.send_text(json.dumps(welcome_message))
            except Exception:
                pass
                
        except Exception as e:
            # Attempt to close connection gracefully if possible
            try:
                await websocket.close(code=1011, reason=f"Connection error: {str(e)}")
            except Exception:
                pass
            raise
    
    def disconnect_monitor(self, monitor_id: str) -> None:
        """Disconnect a monitor"""
        if monitor_id in self.monitor_connections:
            # Store reference to WebSocket object before deletion
            websocket = self.monitor_connections[monitor_id]
            
            # Clean up connections dict first
            del self.monitor_connections[monitor_id]
            
            # Clean up monitor filters
            if monitor_id in self.monitor_filters:
                del self.monitor_filters[monitor_id]
                

            
            # Try to close the connection gracefully (async context but sync function)
            try:
                asyncio.create_task(self._close_websocket(websocket, f"Monitor {monitor_id} disconnected"))
            except Exception:
                pass
    
    def is_worker_connected(self, worker_id: str) -> bool:
        """Check if a worker is connected"""
        return worker_id in self.worker_connections
    
    async def subscribe_to_job(self, client_id: str, job_id: str) -> bool:
        """Subscribe a client to job updates"""
        # Check if client is connected before subscribing
        is_connected = client_id in self.client_connections
        if not is_connected:
            return False
        
        # Record previous subscription if exists
        prev_client = self.job_subscriptions.get(job_id)
        
        # Update subscription mapping
        self.job_subscriptions[job_id] = client_id
        return True
    
    def subscribe_to_stats(self, client_id: str) -> None:
        """Subscribe a client to system stats updates"""
        self.stats_subscriptions.add(client_id)
        
    def unsubscribe_from_stats(self, client_id: str) -> None:
        """Unsubscribe a client from system stats updates"""
        if client_id in self.stats_subscriptions:
            self.stats_subscriptions.remove(client_id)
    
    async def send_to_client(self, client_id: str, message: Any) -> bool:
        """Send a message to a specific client"""
        # Check client connection status
        if client_id not in self.client_connections:
            return False
        
        # Get WebSocket connection
        websocket = self.client_connections[client_id]
        
        try:
            # Serialize the message appropriately
            message_text = None
            message_type = type(message).__name__
            
            if hasattr(message, "json"):
                message_text = message.json()
            elif isinstance(message, dict):
                message_text = json.dumps(message)
            else:
                message_text = str(message)
                
            # Actually send the message
            await websocket.send_text(message_text)
            return True
            
        except RuntimeError as e:
            if "WebSocket is not connected" in str(e) or "Connection is closed" in str(e):
                self.disconnect_client(client_id)
            return False
                
        except Exception:
            return False
    
    async def send_to_worker(self, worker_id: str, message: Any) -> bool:
        """Send a message to a specific worker"""
        # Check worker connection status
        if worker_id not in self.worker_connections:
            return False
        
        # Get WebSocket connection
        websocket = self.worker_connections[worker_id]
        
        try:
            # Serialize the message appropriately
            message_text = None
            message_type = type(message).__name__
            print(f"🔰 Serializing message of type {message_type}...")
            
            if hasattr(message, "json"):
                message_text = message.json()
                print(f"🔰 Used .json() method for serialization")
            elif isinstance(message, dict):
                message_text = json.dumps(message)
                print(f"🔰 Used json.dumps() for dictionary serialization")
            else:
                message_text = str(message)
                print(f"🔰 Used str() conversion for serialization")
                
            # Extract and log message details
            msg_type = "unknown"
            msg_id = "unknown"
            job_type = None
            
            if isinstance(message, dict):
                if "type" in message:
                    msg_type = message["type"]
                if "id" in message:
                    msg_id = message["id"]
                elif "job_id" in message:
                    msg_id = message["job_id"]
                if "job_type" in message:
                    job_type = message["job_type"]
            elif hasattr(message, "type"):
                msg_type = message.type
                if hasattr(message, "id"):
                    msg_id = message.id
                elif hasattr(message, "job_id"):
                    msg_id = message.job_id
                if hasattr(message, "job_type"):
                    job_type = message.job_type
                
            print(f"🔰 Message details: type={msg_type}, id={msg_id}")
            if job_type:
                print(f"🔰 Job type: {job_type}")
                
            # Log message content summary
            if len(message_text) > 1000:
                content_preview = message_text[:500] + "..." + message_text[-500:]
                print(f"🔰 Message content preview (first/last 500 chars): {content_preview}")
            else:
                print(f"🔰 Message content: {message_text}")
                
            print(f"🔰 Message size: {len(message_text)} bytes")
            logger.info(f"[WORKER-MSG] Sending {message_type} message (type: {msg_type}, id: {msg_id}) to worker {worker_id}: {len(message_text)} bytes")
            
            # Actually send the message
            print(f"🔰 Sending message to worker {worker_id}...")
            await websocket.send_text(message_text)
            
            print(f"🔰 Successfully sent message to worker {worker_id} ✅")
            logger.info(f"[WORKER-MSG] Successfully sent message to worker {worker_id}")
            print(f"🔰🔰🔰 END WORKER MESSAGE DELIVERY (SUCCESS) 🔰🔰🔰\n\n")
            return True
            
        except RuntimeError as e:
            if "WebSocket is not connected" in str(e) or "Connection is closed" in str(e):
                self.disconnect_worker(worker_id)
            return False
                
        except Exception:
            return False
    
    async def broadcast_to_clients(self, message: Any) -> int:
        """Broadcast a message to all connected clients"""
        successful_sends = 0
        
        for client_id in list(self.client_connections.keys()):
            if await self.send_to_client(client_id, message):
                successful_sends += 1
        
        return successful_sends
    
    async def broadcast_to_workers(self, message: Any) -> int:
        """Broadcast a message to all connected workers"""
        successful_sends = 0
        
        for worker_id in list(self.worker_connections.keys()):
            if await self.send_to_worker(worker_id, message):
                successful_sends += 1
        
        return successful_sends
    
    async def send_job_update(self, job_id: str, update: Any) -> bool:
        """Send a job update to the subscribed client"""
        try:
            if job_id in self.job_subscriptions:
                client_id = self.job_subscriptions[job_id]
                
                # Verify client connection exists
                if client_id not in self.client_connections:
                    return False
                
                # Send the update
                success = await self.send_to_client(client_id, update)
                return success
            else:
                return False
                
        except Exception:
            return False
    
    async def broadcast_stats(self, stats: Any) -> int:
        """Broadcast stats to all subscribed clients and monitors"""
        successful_sends = 0
        
        # Ensure stats has the correct type
        stats_message = stats
        if isinstance(stats, dict) and "type" not in stats:
            stats_message = stats.copy()
            stats_message["type"] = "stats_response"
        
        # Send to subscribed clients
        for client_id in list(self.stats_subscriptions):
            if await self.send_to_client(client_id, stats_message):
                successful_sends += 1
        
        # Also send to all monitors
        await self.broadcast_to_monitors(stats_message)
        
        return successful_sends
        
    async def broadcast_to_monitors(self, message: Any) -> int:
        """Broadcast a message to all connected monitors"""
        if not self.monitor_connections:
            return 0
            
        # Prepare message for sending
        message_dict = None
        
        # Convert to dict if it's a Pydantic model
        if hasattr(message, 'dict'):
            message_dict = message.dict()
        elif isinstance(message, dict):
            message_dict = message.copy()
        else:
            # Convert to string and wrap in a dict
            message_dict = {"content": str(message), "type": "unknown"}
            
        # Ensure message has a type
        if "type" not in message_dict:
            message_dict["type"] = "unknown"
            
        # Add timestamp if not present
        if "timestamp" not in message_dict:
            message_dict["timestamp"] = time.time()
            
        # Add message_id if not present for tracking
        if "message_id" not in message_dict:
            message_dict["message_id"] = f"{message_dict['type']}-{int(time.time())}"
        
        # Convert to JSON string
        message_json = json.dumps(message_dict)
        
        # Track successful broadcasts
        successful_sends = 0
        skipped_sends = 0
        
        # Get message type for filtering
        message_type = message_dict.get("type", "unknown")
        
        for monitor_id, websocket in list(self.monitor_connections.items()):
            # Check if this monitor has filters and if the message type passes the filter
            filters = self.monitor_filters.get(monitor_id, set())
            
            # If filters exist and message type is not in filters, skip
            if filters and message_type not in filters:
                skipped_sends += 1
                continue
            try:
                # Check if websocket is still open
                if websocket.client_state.name != "CONNECTED":
                    asyncio.create_task(self._schedule_disconnect_monitor(monitor_id))
                    continue
                    
                # Send the message
                await websocket.send_text(message_json)
                successful_sends += 1
                
                # Send an acknowledgment message to confirm successful delivery
                ack_message = {
                    "type": "ack",
                    "timestamp": time.time(),
                    "message_id": f"ack-{int(time.time())}",
                    "original_type": message_type,
                    "original_id": message_dict.get("message_id", "unknown")
                }
                await websocket.send_text(json.dumps(ack_message))
                
            except RuntimeError as e:
                # Schedule disconnection for websocket errors
                if "WebSocket is not connected" in str(e) or "Connection is closed" in str(e):
                    asyncio.create_task(self._schedule_disconnect_monitor(monitor_id))
            except Exception:
                # Schedule disconnection
                asyncio.create_task(self._schedule_disconnect_monitor(monitor_id))
        

        
        return successful_sends
    
    async def _schedule_disconnect_monitor(self, monitor_id: str) -> None:
        """Schedule monitor disconnection to avoid concurrent modification"""
        self.disconnect_monitor(monitor_id)
        
    async def send_system_status_to_monitors(self, redis_service=None) -> None:
        """Send comprehensive system status to all monitors
        
        Args:
            redis_service: Optional RedisService instance to get detailed worker status
        """
        if not self.monitor_connections:
            return
            
        # Build comprehensive status message
        status = {
            "type": "system_status",
            "timestamp": time.time(),
            "message_id": f"status-{int(time.time())}",  # Add unique message ID for tracking
            "connections": {
                "clients": list(self.client_connections.keys()),
                "workers": list(self.worker_connections.keys()),
                "monitors": list(self.monitor_connections.keys())
            },
            "workers": {
                worker_id: {"status": status} 
                for worker_id, status in self.worker_status.items()
            },
            "subscriptions": {
                "stats": list(self.stats_subscriptions),
                "job_notifications": list(self.job_notification_subscriptions),
                "jobs": self.job_subscriptions
            }
        }
        
        # Log the basic status structure
        worker_count = len(status['workers']) if isinstance(status['workers'], (dict, list)) else 0
        print(f"🔄 Created system status with {worker_count} workers")
        
        # Safely access connection counts with type checking
        client_count = len(status['connections']['clients']) if isinstance(status['connections'], dict) and isinstance(status['connections'].get('clients'), (list, set)) else 0
        worker_count = len(status['connections']['workers']) if isinstance(status['connections'], dict) and isinstance(status['connections'].get('workers'), (list, set)) else 0
        monitor_count = len(status['connections']['monitors']) if isinstance(status['connections'], dict) and isinstance(status['connections'].get('monitors'), (list, set)) else 0
        print(f"🔄 Connections: {client_count} clients, {worker_count} workers, {monitor_count} monitors")
        
        # Add detailed worker status from Redis if available
        if redis_service:
            try:
                # Get detailed worker status from Redis
                print(f"🔄 Fetching detailed worker status from Redis service")
                redis_workers_status = redis_service.get_all_workers_status()
                
                # Update the workers section with detailed information
                detailed_workers = {}
                for worker_id, worker_data in redis_workers_status.items():
                    # Combine connection status with Redis status
                    worker_info = worker_data.copy()
                    
                    # Add connection status if we have it
                    if worker_id in self.worker_status:
                        worker_info["connection_status"] = self.worker_status[worker_id]
                    else:
                        worker_info["connection_status"] = "disconnected"
                        
                    detailed_workers[worker_id] = worker_info
                
                # Replace the workers section with detailed information
                status["workers"] = detailed_workers
                
                # Add system stats
                status["system"] = redis_service.get_stats()
                
            except Exception:
                pass
        

        
        # Broadcast to all monitors
        sent_count = await self.broadcast_to_monitors(status)
        
    def subscribe_worker_to_job_notifications(self, worker_id: str) -> None:
        """Subscribe a worker to job notifications"""
        self.job_notification_subscriptions.add(worker_id)
    
    def unsubscribe_worker_from_job_notifications(self, worker_id: str) -> None:
        """Unsubscribe a worker from job notifications"""
        if worker_id in self.job_notification_subscriptions:
            self.job_notification_subscriptions.remove(worker_id)
    
    async def update_worker_status(self, worker_id: str, status: str) -> None:
        """Update a worker's status and broadcast to monitors"""
        if worker_id in self.worker_connections:
            # Update status in memory
            self.worker_status[worker_id] = status
            
            # Create worker status update message
            status_update = {
                "type": "worker_status",
                "timestamp": time.time(),
                "message_id": f"worker-status-{int(time.time())}",
                "worker_id": worker_id,
                "status": status
            }
            
            # Broadcast to all monitors
            await self.broadcast_to_monitors(status_update)
    
    async def notify_idle_workers(self, job_notification: Any) -> int:
        """Send job notification to idle workers subscribed to notifications"""
        successful_sends = 0
        
        # Convert Pydantic model to dict if needed
        if hasattr(job_notification, 'dict'):
            # It's a Pydantic model, convert to dict
            notification_dict = job_notification.dict()
        else:
            # It's already a dict
            notification_dict = job_notification
            
        job_id = notification_dict.get('job_id', 'unknown')
        job_type = notification_dict.get('job_type', 'unknown')
        
        # Ensure the notification has the correct type
        if 'type' not in notification_dict:
            notification_dict['type'] = 'job_available'
        
        # Find eligible idle workers
        idle_workers = []
        for worker_id in list(self.job_notification_subscriptions):
            if worker_id in self.worker_status and self.worker_status[worker_id] == "idle":
                idle_workers.append(worker_id)
        
        if not idle_workers:
            return 0
        
        # Check that workers are actually connected
        for worker_id in idle_workers.copy():
            if worker_id not in self.worker_connections:
                idle_workers.remove(worker_id)
        
        # Create a copy of the notification for each worker to avoid shared references
        # This ensures each worker gets its own notification with accurate worker count
        base_notification = notification_dict.copy()
        
        # Send to eligible workers
        for worker_id in idle_workers:
            # Create a worker-specific notification with accurate worker count
            worker_notification = base_notification.copy()
            # If this is a worker_notification type message, ensure worker_count is correct
            if worker_notification.get('type') == 'worker_notification':
                worker_notification['worker_count'] = len(idle_workers)
            
            success = await self.send_to_worker(worker_id, worker_notification)
            if success:
                successful_sends += 1
        return successful_sends
    
    async def broadcast_job_notification(self, job_notification: Dict[str, Any]) -> int:
        """Broadcast job notification to all workers regardless of status"""
        successful_sends = 0
        
        for worker_id in list(self.worker_connections.keys()):
            if await self.send_to_worker(worker_id, job_notification):
                successful_sends += 1
        
        return successful_sends
        
    def set_monitor_subscriptions(self, monitor_id: str, channels: List[str]) -> None:
        """Set the subscription channels for a monitor
        
        Args:
            monitor_id: The ID of the monitor to set subscriptions for
            channels: List of channel names to subscribe to
        """
        # Check if monitor is connected
        if monitor_id not in self.monitor_connections:
            return
        else:
            print(f"🔍 No previous subscriptions")
            logger.info(f"[MONITOR-SUB] Monitor {monitor_id} setting initial subscriptions to {channels}")
            
        # Update the monitor's filter set
        self.monitor_filters[monitor_id] = set(channels)
        
        # Log the subscription update
        print(f"🔍 Successfully updated subscriptions for monitor {monitor_id} ✅")
        print(f"🔍 Subscribed to channels: {channels}")
        logger.info(f"[MONITOR-SUB] Monitor {monitor_id} subscribed to channels: {channels}")
        
        # Send confirmation message to the monitor
        try:
            confirmation = {
                "type": "subscription_confirmed",
                "monitor_id": monitor_id,
                "channels": channels,
                "timestamp": time.time()
            }
            
            print(f"🔍 Sending confirmation message to monitor...")
            # Create a task to send the confirmation asynchronously
            websocket = self.monitor_connections[monitor_id]
            asyncio.create_task(websocket.send_text(json.dumps(confirmation)))
            
            print(f"🔍 Confirmation message sent successfully ✅")
            logger.info(f"[MONITOR-SUB] Sent subscription confirmation to monitor {monitor_id}")
            
            # Send an immediate system status update to the monitor
            print(f"🔍 Scheduling immediate system status update for monitor...")
            asyncio.create_task(self.send_system_status_to_monitors())
            print(f"🔍 System status update scheduled ✅")
            logger.info(f"[MONITOR-SUB] Scheduled immediate system status update for monitor {monitor_id}")
            
        except Exception as e:
            print(f"🔍 ERROR sending confirmation to monitor: {str(e)} ❌")
            logger.error(f"[MONITOR-SUB] Error sending subscription confirmation to monitor {monitor_id}: {str(e)}")
            import traceback
            traceback.print_exc()
            
        print(f"🔍🔍🔍 END MONITOR SUBSCRIPTION REQUEST 🔍🔍🔍\n\n")
    
    async def _close_websocket(self, websocket: WebSocket, reason: str) -> None:
        """Gracefully close a WebSocket connection with error handling
        
        Args:
            websocket: The WebSocket connection to close
            reason: The reason for closing the connection
        """
        try:
            logger.debug(f"Attempting to close WebSocket connection: {reason}")
            # Use a standard WebSocket close code (1000 = normal closure)
            await websocket.close(code=1000, reason=reason)
            logger.info(f"WebSocket connection closed successfully: {reason}")
        except RuntimeError as e:
            # Handle specific runtime errors like "WebSocket is not connected"
            if "WebSocket is not connected" in str(e) or "Connection is closed" in str(e):
                logger.debug(f"WebSocket already closed: {reason}")
            else:
                logger.warning(f"Runtime error while closing WebSocket: {str(e)}")
        except Exception as e:
            # Handle any other unexpected errors
            logger.warning(f"Error while closing WebSocket: {str(e)}, type: {type(e).__name__}")
    
    async def notify_job_update(self, job_id: str, update: Dict[str, Any]) -> bool:
        """Notify subscribers about a job update.
        
        Args:
            job_id: ID of the job that was updated
            update: Update data
            
        Returns:
            bool: True if update was sent successfully, False otherwise
        """
        # Similar implementation to send_job_update
        try:
            # Check if any clients are subscribed to this job
            if job_id not in self.job_subscriptions:
                logger.debug(f"[JOB-UPDATE] No clients subscribed to job {job_id}")
                return False
                
            # Get the client ID subscribed to this job
            client_id = self.job_subscriptions[job_id]
            
            # Check if the client is still connected
            if client_id not in self.client_connections:
                logger.warning(f"[JOB-UPDATE] Client {client_id} subscribed to job {job_id} is no longer connected")
                # Remove the subscription
                del self.job_subscriptions[job_id]
                return False
            
            # Send the update to the client
            websocket = self.client_connections[client_id]
            await websocket.send_text(json.dumps(update))
            logger.info(f"[JOB-UPDATE] Sent job update for {job_id} to client {client_id}")
            return True
            
        except Exception as e:
            logger.error(f"[JOB-UPDATE] Error sending job update for {job_id}: {str(e)}")
            return False
    
    async def send_to_monitor(self, monitor_id: str, message: Dict[str, Any]) -> bool:
        """Send a message to a specific monitor.
        
        Args:
            monitor_id: ID of the monitor to send the message to
            message: Message to send
            
        Returns:
            bool: True if message was sent successfully, False otherwise
        """
        try:
            # Check if the monitor is connected
            if monitor_id not in self.monitor_connections:
                logger.warning(f"[MONITOR-MSG] Monitor {monitor_id} is not connected")
                return False
            
            # Send the message to the monitor
            websocket = self.monitor_connections[monitor_id]
            await websocket.send_text(json.dumps(message))
            logger.debug(f"[MONITOR-MSG] Sent message to monitor {monitor_id}")
            return True
            
        except Exception as e:
            logger.error(f"[MONITOR-MSG] Error sending message to monitor {monitor_id}: {str(e)}")
            return False
    
    async def subscribe_to_job_notifications(self, worker_id: str, enabled: bool = True) -> bool:
        """Subscribe a worker to job notifications.
        
        Args:
            worker_id: ID of the worker subscribing
            enabled: Whether to enable or disable the subscription
            
        Returns:
            bool: True if subscription was successful, False otherwise
        """
        try:
            # Check if the worker is connected
            if worker_id not in self.worker_connections:
                logger.warning(f"[JOB-NOTIFY] Worker {worker_id} is not connected")
                return False
            
            if enabled:
                # Add the worker to the job notification subscriptions
                self.job_notification_subscriptions.add(worker_id)
                logger.info(f"[JOB-NOTIFY] Worker {worker_id} subscribed to job notifications")
                
                # Send a confirmation message to the worker
                confirmation = {
                    "type": "job_notifications_subscribed",
                    "timestamp": time.time()
                }
                
                websocket = self.worker_connections[worker_id]
                await websocket.send_text(json.dumps(confirmation))
                return True
            else:
                # Remove the worker from job notification subscriptions
                if worker_id in self.job_notification_subscriptions:
                    self.job_notification_subscriptions.remove(worker_id)
                    logger.info(f"[JOB-NOTIFY] Worker {worker_id} unsubscribed from job notifications")
                    return True
                return False
            
        except Exception as e:
            logger.error(f"[JOB-NOTIFY] Error subscribing worker {worker_id} to job notifications: {str(e)}")
            return False
