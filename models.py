#!/usr/bin/env python3
# Core data models for the Redis queue system
import json
import logging
import time
import uuid
from pydantic import BaseModel, Field, validator
from typing import Dict, Any, Optional, List, Union

# Initialize logger
logger = logging.getLogger(__name__)

# Job model for job submission
class Job(BaseModel):
    id: str = Field(default_factory=lambda: f"job-{uuid.uuid4()}")
    type: str
    priority: int = 0
    params: Dict[str, Any]
    client_id: Optional[str] = None

# Worker identification for job processing
class WorkerInfo(BaseModel):
    machine_id: str
    gpu_id: int
    worker_id: Optional[str] = None

    def __init__(self, **data):
        super().__init__(**data)
        if not self.worker_id:
            self.worker_id = f"{self.machine_id}:{self.gpu_id}"

# WebSocket message types
class MessageType:
    # Client to Server Messages
    SUBMIT_JOB = "submit_job"
    GET_JOB_STATUS = "get_job_status"
    REGISTER_WORKER = "register_worker"
    UPDATE_JOB_PROGRESS = "update_job_progress"
    COMPLETE_JOB = "complete_job"
    FAIL_JOB = "fail_job"
    GET_STATS = "get_stats"
    SUBSCRIBE_STATS = "subscribe_stats"
    SUBSCRIBE_JOB = "subscribe_job"
    
    # Worker Status Messages
    WORKER_HEARTBEAT = "worker_heartbeat"
    # HEARTBEAT = "heartbeat"  # Legacy type removed
    WORKER_STATUS = "worker_status"
    CLAIM_JOB = "claim_job"
    JOB_CLAIMED = "job_claimed"
    # SUBSCRIBE_JOB_NOTIFICATIONS = "subscribe_job_notifications"  # Redundant type removed

    # Server to Client Messages
    JOB_ACCEPTED = "job_accepted"
    JOB_STATUS = "job_status"
    JOB_UPDATE = "job_update"
    JOB_ASSIGNED = "job_assigned"
    JOB_COMPLETED = "job_completed"
    STATS_RESPONSE = "stats_response"
    ERROR = "error"
    WORKER_REGISTERED = "worker_registered"
    
    # Server to Worker Notifications
    JOB_AVAILABLE = "job_available"
    
    # Monitor Messages
    STAY_ALIVE = "stay_alive"
    STAY_ALIVE_RESPONSE = "stay_alive_response"
    
    # Connection Messages
    CONNECTION_ESTABLISHED = "connection_established"

# Base message class
class BaseMessage(BaseModel):
    type: str
    timestamp: float = Field(default_factory=time.time)

# Core Client to Server Messages
class SubmitJobMessage(BaseMessage):
    type: str = MessageType.SUBMIT_JOB
    job_type: str
    priority: int = 0
    payload: Dict[str, Any]
    
    @validator('priority')
    def validate_priority(cls, v):
        if v is None:
            return 0
        try:
            return int(v)
        except (ValueError, TypeError):
            logger.warning(f"Could not convert priority {v} to integer, using 0")
            return 0

class GetJobStatusMessage(BaseMessage):
    type: str = MessageType.GET_JOB_STATUS
    job_id: str

class RegisterWorkerMessage(BaseMessage):
    type: str = MessageType.REGISTER_WORKER
    machine_id: str
    gpu_id: int

class UpdateJobProgressMessage(BaseMessage):
    type: str = MessageType.UPDATE_JOB_PROGRESS
    job_id: str
    machine_id: str
    gpu_id: int
    progress: int
    status: str = "processing"
    message: Optional[str] = None

class CompleteJobMessage(BaseMessage):
    type: str = MessageType.COMPLETE_JOB
    job_id: str
    machine_id: str
    gpu_id: int
    result: Optional[Dict[str, Any]] = None

# Core Server to Client Messages
class JobAcceptedMessage(BaseMessage):
    type: str = MessageType.JOB_ACCEPTED
    job_id: str
    status: str = "pending"
    position: Optional[int] = None
    estimated_start: Optional[str] = None
    notified_workers: Optional[int] = 0  # Number of workers notified about this job

class JobStatusMessage(BaseMessage):
    type: str = MessageType.JOB_STATUS
    job_id: str
    status: str
    progress: Optional[int] = None
    worker_id: Optional[str] = None
    started_at: Optional[float] = None
    completed_at: Optional[float] = None
    result: Optional[Dict[str, Any]] = None
    message: Optional[str] = None

class JobUpdateMessage(BaseMessage):
    type: str = MessageType.JOB_UPDATE
    job_id: str
    status: str
    priority: Optional[int] = None
    position: Optional[int] = None
    progress: Optional[int] = None
    eta: Optional[str] = None
    message: Optional[str] = None

class JobAssignedMessage(BaseMessage):
    type: str = MessageType.JOB_ASSIGNED
    job_id: str
    job_type: str
    priority: int
    params: Dict[str, Any]

class JobCompletedMessage(BaseMessage):
    type: str = MessageType.JOB_COMPLETED
    job_id: str
    status: str = "completed"
    priority: Optional[int] = None
    position: Optional[int] = None
    result: Optional[Dict[str, Any]] = None

class ErrorMessage(BaseMessage):
    type: str = MessageType.ERROR
    error: str
    details: Optional[Dict[str, Any]] = None

class WorkerRegisteredMessage(BaseMessage):
    type: str = MessageType.WORKER_REGISTERED
    worker_id: str
    status: str = "active"

def parse_message(data: Dict[str, Any]) -> Optional[BaseMessage]:
    """Parse incoming message data into appropriate message model"""
    if not isinstance(data, dict) or "type" not in data:
        logger.error(f"Invalid message format: {data}")
        return None
    
    message_type = data.get("type")
    logger.info(f"Received message of type: {message_type}")
    
    # Simply log the raw message data without any parsing
    print(f"\n\n***** PROCESSING RAW MESSAGE *****")
    print(f"***** MESSAGE TYPE: {message_type} *****")
    print(f"***** MESSAGE DATA: {json.dumps(data, indent=2)} *****")
    print("*************************************\n\n")
    
    # For submit_job messages
    if message_type == MessageType.SUBMIT_JOB:
        try:
            # Create a minimal SubmitJobMessage with just the essential fields
            result = SubmitJobMessage(
                type=MessageType.SUBMIT_JOB,
                job_type=data.get("job_type", "unknown"),
                priority=data.get("priority", 0),
                payload=data.get("payload", {})
            )
            logger.info(f"Created submit_job message: {result}")
            return result
        except Exception as e:
            logger.error(f"Error creating submit_job message: {str(e)}")
            return None
    
    # For get_job_status messages
    elif message_type == MessageType.GET_JOB_STATUS:
        try:
            result = GetJobStatusMessage(
                type=MessageType.GET_JOB_STATUS,
                job_id=data.get("job_id", "")
            )
            return result
        except Exception as e:
            logger.error(f"Error creating get_job_status message: {str(e)}")
            return None
    
    # For subscribe_job messages
    elif message_type == MessageType.SUBSCRIBE_JOB:
        try:
            result = SubscribeJobMessage(
                type=MessageType.SUBSCRIBE_JOB,
                job_id=data.get("job_id", "")
            )
            return result
        except Exception as e:
            logger.error(f"Error creating subscribe_job message: {str(e)}")
            return None
    
    # For subscribe_stats messages
    elif message_type == MessageType.SUBSCRIBE_STATS:
        try:
            result = SubscribeStatsMessage(
                type=MessageType.SUBSCRIBE_STATS
            )
            return result
        except Exception as e:
            logger.error(f"Error creating subscribe_stats message: {str(e)}")
            return None
    
    # For get_stats messages
    elif message_type == MessageType.GET_STATS:
        try:
            result = GetStatsMessage(
                type=MessageType.GET_STATS
            )
            return result
        except Exception as e:
            logger.error(f"Error creating get_stats message: {str(e)}")
            return None
    
    # For connection_established messages
    elif message_type == MessageType.CONNECTION_ESTABLISHED:
        try:
            result = ConnectionEstablishedMessage(
                type=MessageType.CONNECTION_ESTABLISHED,
                message=data.get("message", "Connection established")
            )
            return result
        except Exception as e:
            logger.error(f"Error creating connection_established message: {str(e)}")
            return None
    
    # For other message types, return None
    else:
        logger.warning(f"Unhandled message type: {message_type}")
        return None

# Simple stats models (basic versions for core functionality)
class SubscribeStatsMessage(BaseMessage):
    type: str = MessageType.SUBSCRIBE_STATS
    enabled: bool = True

class GetStatsMessage(BaseMessage):
    type: str = MessageType.GET_STATS

class SubscribeJobMessage(BaseMessage):
    type: str = MessageType.SUBSCRIBE_JOB
    job_id: str
    
# Worker Heartbeat and Status Messages
class WorkerHeartbeatMessage(BaseMessage):
    type: str = MessageType.WORKER_HEARTBEAT
    worker_id: str
    status: Optional[str] = "idle"
    load: Optional[float] = 0.0
    
# Legacy Worker Heartbeat Message removed - use WorkerHeartbeatMessage instead
    
# Worker Status Message
class WorkerStatusMessage(BaseMessage):
    type: str = MessageType.WORKER_STATUS
    worker_id: str
    status: Optional[str] = "idle"
    capabilities: Optional[Dict[str, Any]] = None
    timestamp: Optional[float] = Field(default_factory=time.time)
    
class ClaimJobMessage(BaseMessage):
    type: str = MessageType.CLAIM_JOB
    worker_id: str
    job_id: str
    claim_timeout: Optional[int] = 30
    
class JobClaimedMessage(BaseMessage):
    type: str = MessageType.JOB_CLAIMED
    job_id: str
    worker_id: str
    success: bool
    job_data: Optional[Dict[str, Any]] = None
    message: Optional[str] = None
    
# SubscribeJobNotificationsMessage removed - workers automatically receive job notifications
    
# Monitor Messages
class StayAliveMessage(BaseMessage):
    type: str = MessageType.STAY_ALIVE
    monitor_id: str
    timestamp: float = Field(default_factory=time.time)

class StayAliveResponseMessage(BaseMessage):
    type: str = MessageType.STAY_ALIVE_RESPONSE
    timestamp: float = Field(default_factory=time.time)

# Job Notification Messages
class JobAvailableMessage(BaseMessage):
    type: str = MessageType.JOB_AVAILABLE
    job_id: str
    job_type: str
    priority: Optional[int] = 0
    params_summary: Optional[Dict[str, Any]] = None

# Connection Messages
class ConnectionEstablishedMessage(BaseMessage):
    type: str = MessageType.CONNECTION_ESTABLISHED
    message: str = "Connection established"
