#!/usr/bin/env python3
# Implementation of the MessageModelsInterface
from typing import Dict, Any, Optional, List, Union
from pydantic import BaseModel

from .interfaces.message_models_interface import MessageModelsInterface
from .models import (
    BaseMessage, ErrorMessage, JobAcceptedMessage, JobStatusMessage,
    JobUpdateMessage, WorkerRegisteredMessage, JobAvailableMessage,
    JobCompletedMessage, SubmitJobMessage, GetJobStatusMessage,
    SubscribeJobMessage, SubscribeStatsMessage, GetStatsMessage,
    ConnectionEstablishedMessage, MessageType
)
from .utils.logger import logger

class MessageModels(MessageModelsInterface):
    """
    Implementation of the MessageModelsInterface.
    
    This class provides concrete implementations of the message model
    creation and validation methods defined in the interface.
    """
    
    def parse_message(self, data: Dict[str, Any]) -> Optional[BaseModel]:
        """
        Parse incoming message data into appropriate message model.
        
        Args:
            data: Raw message data
            
        Returns:
            Optional[BaseModel]: Parsed message model if valid, None otherwise
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
