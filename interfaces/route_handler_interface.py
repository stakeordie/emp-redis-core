#!/usr/bin/env python3
# Interface for route handling
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, Callable, Awaitable
from fastapi import FastAPI, WebSocket

class RouteHandlerInterface(ABC):
    """
    Interface defining the contract for route handling.
    
    This interface ensures that all route handler implementations
    follow the same contract, making it easier to organize and
    maintain route handling logic.
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
    async def start_background_tasks(self) -> None:
        """
        Start background tasks for the application.
        
        This method should initialize and start any background tasks
        needed for the application, such as periodic stats broadcasts,
        cleanup tasks, etc.
        """
        pass
    
    @abstractmethod
    async def stop_background_tasks(self) -> None:
        """
        Stop background tasks for the application.
        
        This method should properly stop and clean up any background
        tasks started by the application.
        """
        pass
    
    @abstractmethod
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
        pass
    
    @abstractmethod
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
        pass
    
    @abstractmethod
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
        pass
