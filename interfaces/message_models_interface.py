#!/usr/bin/env python3
# Interface for message models
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List, Union
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
                                   position: Optional[int] = None) -> BaseModel:
        """
        Create a job accepted message.
        
        Args:
            job_id: ID of the accepted job
            status: Job status
            position: Optional position in queue
            
        Returns:
            BaseModel: Job accepted message model
        """
        pass
    
    @abstractmethod
    def create_job_status_message(self, job_id: str, status: str, 
                                 progress: Optional[int] = None,
                                 result: Optional[Dict[str, Any]] = None) -> BaseModel:
        """
        Create a job status message.
        
        Args:
            job_id: ID of the job
            status: Job status
            progress: Optional progress percentage
            result: Optional job result
            
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
