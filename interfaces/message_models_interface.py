#!/usr/bin/env python3
# Interface for message models
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List, Union, Type
from pydantic import BaseModel

class MessageModelsInterface(ABC):
    """
    Interface defining the contract for message models.
    
    This interface ensures that all message model implementations
    follow the same contract, making it easier to validate and
    process messages consistently.
    """
    
    @abstractmethod
    def parse_message(self, data: Dict[str, Any]) -> Optional[BaseModel]:
        """
        Parse incoming message data into appropriate message model.
        
        Args:
            data: Raw message data
            
        Returns:
            Optional[BaseModel]: Parsed message model if valid, None otherwise
        """
        pass
    
    @abstractmethod
    def create_error_message(self, error: str, details: Optional[Dict[str, Any]] = None) -> BaseModel:
        """
        Create an error message.
        
        Args:
            error: Error message
            details: Optional error details
            
        Returns:
            BaseModel: Error message model
        """
        pass
    
    @abstractmethod
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
        pass
    
    @abstractmethod
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
        pass
    
    @abstractmethod
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
        pass
    
    @abstractmethod
    def create_stats_response_message(self, stats: Dict[str, Any]) -> BaseModel:
        """
        Create a stats response message.
        
        Args:
            stats: System statistics
            
        Returns:
            BaseModel: Stats response message model
        """
        pass
    
    @abstractmethod
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
        pass
    
    @abstractmethod
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
        pass
    
    @abstractmethod
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
        pass
    
    @abstractmethod
    def validate_submit_job_message(self, data: Dict[str, Any]) -> bool:
        """
        Validate a submit job message.
        
        Args:
            data: Message data
            
        Returns:
            bool: True if valid, False otherwise
        """
        pass
    
    @abstractmethod
    def validate_get_job_status_message(self, data: Dict[str, Any]) -> bool:
        """
        Validate a get job status message.
        
        Args:
            data: Message data
            
        Returns:
            bool: True if valid, False otherwise
        """
        pass
    
    @abstractmethod
    def validate_register_worker_message(self, data: Dict[str, Any]) -> bool:
        """
        Validate a register worker message.
        
        Args:
            data: Message data
            
        Returns:
            bool: True if valid, False otherwise
        """
        pass
