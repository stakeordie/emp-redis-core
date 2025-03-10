#!/usr/bin/env python3
# Implementation of the MessageModelsInterface
import json
import time
import uuid
from typing import Dict, Any, Optional, List, Union
from pydantic import BaseModel, Field, field_validator
from .interfaces.message_models_interface import MessageModelsInterface
from .utils.logger import logger

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
    WORKER_STATUS = "worker_status"
    CLAIM_JOB = "claim_job"
    JOB_CLAIMED = "job_claimed"
    
    # Server to Client Messages
    JOB_ACCEPTED = "job_accepted"
    JOB_STATUS = "job_status"
    JOB_UPDATE = "job_update"
    JOB_ASSIGNED = "job_assigned"
    JOB_COMPLETED = "job_completed"
    JOB_AVAILABLE = "job_available"
    WORKER_REGISTERED = "worker_registered"
    ERROR = "error"
    STATS_RESPONSE = "stats_response"
    
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
    
    @field_validator('priority')
    def validate_priority(cls, v):
        if v < 0 or v > 10:
            raise ValueError("Priority must be between 0 and 10")
        return v

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
    notified_workers: Optional[int] = 0

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
    worker_id: Optional[str] = None

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

# Simple stats models
class SubscribeStatsMessage(BaseMessage):
    type: str = MessageType.SUBSCRIBE_STATS
    enabled: bool = True

class GetStatsMessage(BaseMessage):
    type: str = MessageType.GET_STATS

class StatsResponseMessage(BaseMessage):
    type: str = MessageType.STATS_RESPONSE
    stats: Dict[str, Any]

class SubscribeJobMessage(BaseMessage):
    type: str = MessageType.SUBSCRIBE_JOB
    job_id: str
    
# Worker Heartbeat and Status Messages
class WorkerHeartbeatMessage(BaseMessage):
    type: str = MessageType.WORKER_HEARTBEAT
    worker_id: str
    status: Optional[str] = "idle"
    load: Optional[float] = 0.0
    
# Worker Status Message
class WorkerStatusMessage(BaseMessage):
    type: str = MessageType.WORKER_STATUS
    worker_id: str
    status: Optional[str] = "idle"
    capabilities: Optional[Dict[str, Any]] = None

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
    priority: int | None = 0
    job_request_payload: Dict[str, Any] | None = None

# Connection Messages
class ConnectionEstablishedMessage(BaseMessage):
    type: str = MessageType.CONNECTION_ESTABLISHED
    message: str = "Connection established"

class MessageModels(MessageModelsInterface):
    """
    Implementation of the MessageModelsInterface.
    
    This class provides concrete implementations of the message model
    creation and validation methods defined in the interface.
    """
    
    def parse_message(self, data: Dict[str, Any]) -> Optional[BaseMessage]:
        """
        Parse incoming message data into appropriate message model.
        
        Args:
            data: Raw message data
            
        Returns:
            Optional[BaseMessage]: Parsed message model if valid, None otherwise
        """
        if not isinstance(data, dict) or "type" not in data:
            return None
        
        message_type = data.get("type")
        
        # For submit_job messages
        if message_type == MessageType.SUBMIT_JOB:
            try:
                return SubmitJobMessage(
                    type=MessageType.SUBMIT_JOB,
                    job_type=data.get("job_type", "unknown"),
                    priority=data.get("priority", 0),
                    payload=data.get("payload", {})
                )
            except Exception as e:
                logger.error(f"Error parsing submit_job message: {e}")
                return None
        
        # For get_job_status messages
        elif message_type == MessageType.GET_JOB_STATUS:
            try:
                return GetJobStatusMessage(
                    type=MessageType.GET_JOB_STATUS,
                    job_id=data.get("job_id", "")
                )
            except Exception as e:
                logger.error(f"Error parsing get_job_status message: {e}")
                return None
        
        # For subscribe_job messages
        elif message_type == MessageType.SUBSCRIBE_JOB:
            try:
                return SubscribeJobMessage(
                    type=MessageType.SUBSCRIBE_JOB,
                    job_id=data.get("job_id", "")
                )
            except Exception as e:
                logger.error(f"Error parsing subscribe_job message: {e}")
                return None
        
        # For subscribe_stats messages
        elif message_type == MessageType.SUBSCRIBE_STATS:
            try:
                return SubscribeStatsMessage(
                    type=MessageType.SUBSCRIBE_STATS
                )
            except Exception as e:
                logger.error(f"Error parsing subscribe_stats message: {e}")
                return None
        
        # For get_stats messages
        elif message_type == MessageType.GET_STATS:
            try:
                return GetStatsMessage(
                    type=MessageType.GET_STATS
                )
            except Exception as e:
                logger.error(f"Error parsing get_stats message: {e}")
                return None
        
        # For connection_established messages
        elif message_type == MessageType.CONNECTION_ESTABLISHED:
            try:
                return ConnectionEstablishedMessage(
                    type=MessageType.CONNECTION_ESTABLISHED,
                    message=data.get("message", "Connection established")
                )
            except Exception as e:
                logger.error(f"Error parsing connection_established message: {e}")
                return None
        
        # For other message types, return None
        else:
            logger.warning(f"Unknown message type: {message_type}")
            return None
    
    def create_error_message(self, error: str, details: Optional[Dict[str, Any]] = None) -> BaseModel:
        """
        Create an error message.
        
        Args:
            error: Error message
            details: Optional error details
            
        Returns:
            BaseModel: Error message model
        """
        return ErrorMessage(
            type=MessageType.ERROR,
            error=error,
            details=details
        )
    
    def create_job_accepted_message(self, job_id: str, status: str = "pending", 
                                   position: Optional[int] = None,
                                   estimated_start: Optional[str] = None,
                                   notified_workers: Optional[int] = 0) -> BaseModel:
        """
        Create a job accepted message.
        
        Args:
            job_id: ID of the accepted job
            status: Job status
            position: Optional position in queue
            estimated_start: Optional estimated start time
            notified_workers: Optional number of workers notified
            
        Returns:
            BaseModel: Job accepted message model
        """
        return JobAcceptedMessage(
            type=MessageType.JOB_ACCEPTED,
            job_id=job_id,
            status=status,
            position=position,
            estimated_start=estimated_start,
            notified_workers=notified_workers
        )
    
    def create_job_status_message(self, job_id: str, status: str, 
                                 progress: Optional[int] = None,
                                 worker_id: Optional[str] = None,
                                 started_at: Optional[float] = None,
                                 completed_at: Optional[float] = None,
                                 result: Optional[Dict[str, Any]] = None,
                                 message: Optional[str] = None) -> BaseModel:
        """
        Create a job status message.
        
        Args:
            job_id: ID of the job
            status: Job status
            progress: Optional progress percentage
            worker_id: Optional ID of the worker processing the job
            started_at: Optional timestamp when job started
            completed_at: Optional timestamp when job completed
            result: Optional job result
            message: Optional status message
            
        Returns:
            BaseModel: Job status message model
        """
        return JobStatusMessage(
            type=MessageType.JOB_STATUS,
            job_id=job_id,
            status=status,
            progress=progress,
            worker_id=worker_id,
            started_at=started_at,
            completed_at=completed_at,
            result=result,
            message=message
        )
    
    def create_job_update_message(self, job_id: str, status: str,
                                 priority: Optional[int] = None,
                                 position: Optional[int] = None,
                                 progress: Optional[int] = None,
                                 eta: Optional[str] = None,
                                 message: Optional[str] = None,
                                 worker_id: Optional[str] = None) -> BaseModel:
        """
        Create a job update message.
        
        Args:
            job_id: ID of the job
            status: Job status
            priority: Optional job priority
            position: Optional position in queue
            progress: Optional progress percentage
            eta: Optional estimated time of completion
            message: Optional status message
            worker_id: Optional ID of the worker processing the job
            
        Returns:
            BaseModel: Job update message model
        """
        return JobUpdateMessage(
            type=MessageType.JOB_UPDATE,
            job_id=job_id,
            status=status,
            priority=priority,
            position=position,
            progress=progress,
            eta=eta,
            message=message,
            worker_id=worker_id
        )
    
    def create_worker_registered_message(self, worker_id: str, 
                                        status: str = "active") -> BaseModel:
        """
        Create a worker registered message.
        
        Args:
            worker_id: ID of the registered worker
            status: Worker status
            
        Returns:
            BaseModel: Worker registered message model
        """
        return WorkerRegisteredMessage(
            type=MessageType.WORKER_REGISTERED,
            worker_id=worker_id,
            status=status
        )
    
    def create_job_available_message(self, job_id: str, job_type: str,
                                    priority: Optional[int] = 0,
                                    job_request_payload: Optional[Dict[str, Any]] = None) -> BaseModel:
        """
        Create a job available message.
        
        Args:
            job_id: ID of the available job
            job_type: Type of the job
            priority: Optional job priority
            job_request_payload: Optional job request payload
            
        Returns:
            BaseModel: Job available message model
        """
        return JobAvailableMessage(
            type=MessageType.JOB_AVAILABLE,
            job_id=job_id,
            job_type=job_type,
            priority=priority,
            job_request_payload=job_request_payload
        )
    
    def create_job_completed_message(self, job_id: str, status: str = "completed",
                                    priority: Optional[int] = None,
                                    position: Optional[int] = None,
                                    result: Optional[Dict[str, Any]] = None) -> BaseModel:
        """
        Create a job completed message.
        
        Args:
            job_id: ID of the completed job
            status: Job status
            priority: Optional job priority
            position: Optional position in queue
            result: Optional job result
            
        Returns:
            BaseModel: Job completed message model
        """
        return JobCompletedMessage(
            type=MessageType.JOB_COMPLETED,
            job_id=job_id,
            status=status,
            priority=priority,
            position=position,
            result=result
        )
    
    def validate_submit_job_message(self, data: Dict[str, Any]) -> bool:
        """
        Validate a submit job message.
        
        Args:
            data: Message data to validate
            
        Returns:
            bool: True if valid, False otherwise
        """
        if not isinstance(data, dict):
            return False
        
        required_fields = ["job_type", "payload"]
        for field in required_fields:
            if field not in data:
                return False
        
        return True
    
    def validate_get_job_status_message(self, data: Dict[str, Any]) -> bool:
        """
        Validate a get job status message.
        
        Args:
            data: Message data to validate
            
        Returns:
            bool: True if valid, False otherwise
        """
        if not isinstance(data, dict):
            return False
        
        # Check for required fields
        if "job_id" not in data:
            return False
        
        # Ensure job_id is a string
        if not isinstance(data["job_id"], str):
            return False
        
        return True
    
    def validate_register_worker_message(self, data: Dict[str, Any]) -> bool:
        """
        Validate a register worker message.
        
        Args:
            data: Message data to validate
            
        Returns:
            bool: True if valid, False otherwise
        """
        if not isinstance(data, dict):
            return False
        
        # Check for required fields
        required_fields = ["machine_id", "gpu_id"]
        for field in required_fields:
            if field not in data:
                return False
        
        # Ensure machine_id is a string
        if not isinstance(data["machine_id"], str):
            return False
        
        # Ensure gpu_id is an integer
        if not isinstance(data["gpu_id"], int):
            return False
        
        return True
    
    def create_stats_response_message(self, stats: Dict[str, Any]) -> BaseModel:
        """
        Create a stats response message.
        
        Args:
            stats: System statistics
            
        Returns:
            BaseModel: Stats response message model
        """
        return StatsResponseMessage(
            type=MessageType.STATS_RESPONSE,
            stats=stats,
            timestamp=time.time()
        )
