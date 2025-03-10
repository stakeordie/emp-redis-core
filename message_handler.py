#!/usr/bin/env python3
# Implementation of the MessageHandlerInterface
from typing import Dict, Any, Optional, Callable, Awaitable, Dict
from fastapi import WebSocket

from .interfaces.message_handler_interface import MessageHandlerInterface
from .utils.logger import logger

class MessageHandler(MessageHandlerInterface):
    """
    Implementation of the MessageHandlerInterface.
    
    This class provides a concrete implementation of the message handling
    contract defined in the interface, allowing for registration and
    execution of handlers for different message types.
    """
    
    def __init__(self):
        """
        Initialize the message handler with an empty handlers dictionary.
        """
        # Dictionary to store message type -> handler function mappings
        self._handlers: Dict[str, Callable[[WebSocket, Dict[str, Any], str], Awaitable[Optional[Dict[str, Any]]]]] = {}
    
    async def handle_message(self, websocket: WebSocket, message: Dict[str, Any], 
                           connection_id: str) -> Optional[Dict[str, Any]]:
        """
        Handle an incoming message by routing it to the appropriate handler.
        
        Args:
            websocket: WebSocket connection that received the message
            message: Message data
            connection_id: ID of the connection (client_id, worker_id, etc.)
            
        Returns:
            Optional[Dict[str, Any]]: Optional response message
        """
        # Extract the message type
        if not isinstance(message, dict) or "type" not in message:
            logger.warning(f"Invalid message format from {connection_id}: {message}")
            return {"type": "error", "error": "Invalid message format"}
        
        # We know type exists in message because of the check above
        # Use type assertion (cast) to tell the type checker this is a string
        message_type = message.get("type")
        assert isinstance(message_type, str), "message_type must be a string"
        
        # Get the handler for this message type
        handler = self.get_handler(message_type)
        
        # If no handler is registered, return an error
        if handler is None:
            logger.warning(f"No handler registered for message type: {message_type}")
            return {"type": "error", "error": f"Unsupported message type: {message_type}"}
        
        try:
            # Call the handler with the message
            return await handler(websocket, message, connection_id)
        except Exception as e:
            # Log any exceptions that occur during handling
            logger.error(f"Error handling message of type {message_type}: {e}")
            return {"type": "error", "error": f"Error processing message: {str(e)}"}
    
    def register_handler(self, message_type: str, 
                        handler: Callable[[WebSocket, Dict[str, Any], str], Awaitable[Optional[Dict[str, Any]]]]) -> None:
        """
        Register a handler function for a specific message type.
        
        Args:
            message_type: Type of message to handle
            handler: Handler function that takes (websocket, message, connection_id) and returns a response
        """
        # Store the handler in the handlers dictionary
        self._handlers[message_type] = handler
        logger.info(f"Registered handler for message type: {message_type}")
    
    def get_handler(self, message_type: str) -> Optional[Callable[[WebSocket, Dict[str, Any], str], Awaitable[Optional[Dict[str, Any]]]]]:
        """
        Get the handler function for a specific message type.
        
        Args:
            message_type: Type of message to get handler for
            
        Returns:
            Optional[Callable]: Handler function if registered, None otherwise
        """
        # Return the handler from the handlers dictionary, or None if not found
        return self._handlers.get(message_type)
