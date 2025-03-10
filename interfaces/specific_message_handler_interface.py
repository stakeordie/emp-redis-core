#!/usr/bin/env python3
# Interface for specific message handling
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
from fastapi import FastAPI, WebSocket

class SpecificMessageHandlerInterface(ABC):
    """
    Interface defining the contract for handling specific message types.
    
    This interface ensures that all specific message handler implementations
    follow the same contract, making it easier to organize and
    maintain message handling logic.
    """
    
    @abstractmethod
    def init_routes(self, app: FastAPI) -> None:
        """
        Initialize routes for the FastAPI application.
        
        Args:
            app: FastAPI application instance
        """
        pass
    
    @abstractmethod
    async def client_websocket(self, websocket: WebSocket, client_id: str) -> None:
        """
        Handle client WebSocket connections.
        
        Args:
            websocket: WebSocket connection
            client_id: Client identifier
        """
        pass
    
    @abstractmethod
    async def worker_websocket(self, websocket: WebSocket, worker_id: str) -> None:
        """
        Handle worker WebSocket connections.
        
        Args:
            websocket: WebSocket connection
            worker_id: Worker identifier
        """
        pass
    
    @abstractmethod
    async def monitor_websocket(self, websocket: WebSocket, monitor_id: str) -> None:
        """
        Handle monitor WebSocket connections.
        
        Args:
            websocket: WebSocket connection
            monitor_id: Monitor identifier
        """
        pass
    
    @abstractmethod
    async def handle_submit_job(self, client_id: str, message: Dict[str, Any]) -> None:
        """
        Handle job submission from a client.
        
        Args:
            client_id: Client identifier
            message: Submit job message
        """
        pass
    
    @abstractmethod
    async def handle_get_job_status(self, client_id: str, job_id: str) -> None:
        """
        Handle job status request from a client.
        
        Args:
            client_id: Client identifier
            job_id: Job identifier
        """
        pass
    
    @abstractmethod
    async def handle_register_worker(self, worker_id: str, message: Dict[str, Any]) -> None:
        """
        Handle worker registration.
        
        Args:
            worker_id: Worker identifier
            message: Register worker message
        """
        pass
