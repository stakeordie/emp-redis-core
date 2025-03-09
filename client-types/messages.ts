/**
 * TypeScript definitions for Emp-Redis message types
 * 
 * This file provides type definitions for all messages exchanged between
 * clients, workers, and the Redis Hub. It mirrors the Python models defined
 * in core/models.py.
 */

/**
 * All possible message types in the system
 */
export enum MessageType {
  // Client to Server Messages
  SUBMIT_JOB = "submit_job",
  GET_JOB_STATUS = "get_job_status",
  REGISTER_WORKER = "register_worker",
  UPDATE_JOB_PROGRESS = "update_job_progress",
  COMPLETE_JOB = "complete_job",
  FAIL_JOB = "fail_job",
  GET_STATS = "get_stats",
  SUBSCRIBE_STATS = "subscribe_stats",
  SUBSCRIBE_JOB = "subscribe_job",
  
  // Worker Status Messages
  WORKER_HEARTBEAT = "worker_heartbeat",
  // HEARTBEAT = "heartbeat", // Legacy type removed
  WORKER_STATUS = "worker_status",
  CLAIM_JOB = "claim_job",
  JOB_CLAIMED = "job_claimed",
  // SUBSCRIBE_JOB_NOTIFICATIONS = "subscribe_job_notifications", // Redundant type removed

  // Server to Client Messages
  JOB_ACCEPTED = "job_accepted",
  JOB_STATUS = "job_status",
  JOB_UPDATE = "job_update",
  JOB_ASSIGNED = "job_assigned",
  JOB_COMPLETED = "job_completed",
  STATS_RESPONSE = "stats_response",
  ERROR = "error",
  WORKER_REGISTERED = "worker_registered",
  
  // Server to Worker Notifications
  JOB_AVAILABLE = "job_available",
  
  // Monitor Messages
  STAY_ALIVE = "stay_alive",
  STAY_ALIVE_RESPONSE = "stay_alive_response",
  
  // Connection Messages
  CONNECTION_ESTABLISHED = "connection_established"
}

/**
 * Base interface for all messages
 */
export interface BaseMessage {
  type: string;
  timestamp?: number;
}

/**
 * Job model for job submission
 */
export interface Job {
  id: string;
  type: string;
  priority: number;
  params: Record<string, any>;
  client_id?: string;
}

/**
 * Worker identification for job processing
 */
export interface WorkerInfo {
  machine_id: string;
  gpu_id: number;
  worker_id?: string;
}

// Client to Server Messages

/**
 * Message to submit a new job
 */
export interface SubmitJobMessage extends BaseMessage {
  type: MessageType.SUBMIT_JOB;
  job_type: string;
  priority: number;
  payload: Record<string, any>;
}

/**
 * Message to get the status of a job
 */
export interface GetJobStatusMessage extends BaseMessage {
  type: MessageType.GET_JOB_STATUS;
  job_id: string;
}

/**
 * Message to register a worker
 */
export interface RegisterWorkerMessage extends BaseMessage {
  type: MessageType.REGISTER_WORKER;
  machine_id: string;
  gpu_id: number;
}

/**
 * Message to update job progress
 */
export interface UpdateJobProgressMessage extends BaseMessage {
  type: MessageType.UPDATE_JOB_PROGRESS;
  job_id: string;
  machine_id: string;
  gpu_id: number;
  progress: number;
  status: string;
  message?: string;
}

/**
 * Message to mark a job as completed
 */
export interface CompleteJobMessage extends BaseMessage {
  type: MessageType.COMPLETE_JOB;
  job_id: string;
  machine_id: string;
  gpu_id: number;
  result?: Record<string, any>;
}

// Server to Client Messages

/**
 * Message indicating a job has been accepted
 */
export interface JobAcceptedMessage extends BaseMessage {
  type: MessageType.JOB_ACCEPTED;
  job_id: string;
  status: string;
  position?: number;
  estimated_start?: string;
  notified_workers?: number;
}

/**
 * Message with job status information
 */
export interface JobStatusMessage extends BaseMessage {
  type: MessageType.JOB_STATUS;
  job_id: string;
  status: string;
  progress?: number;
  worker_id?: string;
  started_at?: number;
  completed_at?: number;
  result?: Record<string, any>;
  message?: string;
}

/**
 * Message with job update information
 */
export interface JobUpdateMessage extends BaseMessage {
  type: MessageType.JOB_UPDATE;
  job_id: string;
  status: string;
  priority?: number;
  position?: number;
  progress?: number;
  eta?: string;
  message?: string;
}

/**
 * Message indicating a job has been assigned to a worker
 */
export interface JobAssignedMessage extends BaseMessage {
  type: MessageType.JOB_ASSIGNED;
  job_id: string;
  job_type: string;
  priority: number;
  params: Record<string, any>;
}

/**
 * Message indicating a job has been completed
 */
export interface JobCompletedMessage extends BaseMessage {
  type: MessageType.JOB_COMPLETED;
  job_id: string;
  status: string;
  priority?: number;
  position?: number;
  result?: Record<string, any>;
}

/**
 * Error message
 */
export interface ErrorMessage extends BaseMessage {
  type: MessageType.ERROR;
  error: string;
  details?: Record<string, any>;
}

/**
 * Message indicating a worker has been registered
 */
export interface WorkerRegisteredMessage extends BaseMessage {
  type: MessageType.WORKER_REGISTERED;
  worker_id: string;
  status: string;
}

// Stats Messages

/**
 * Message to subscribe to stats updates
 */
export interface SubscribeStatsMessage extends BaseMessage {
  type: MessageType.SUBSCRIBE_STATS;
  enabled: boolean;
}

/**
 * Message to request current stats
 */
export interface GetStatsMessage extends BaseMessage {
  type: MessageType.GET_STATS;
}

/**
 * Message to subscribe to job updates
 */
export interface SubscribeJobMessage extends BaseMessage {
  type: MessageType.SUBSCRIBE_JOB;
  job_id: string;
}

// Worker Messages

/**
 * Worker heartbeat message
 */
export interface WorkerHeartbeatMessage extends BaseMessage {
  type: MessageType.WORKER_HEARTBEAT;
  worker_id: string;
  status?: string;
  load?: number;
}

/**
 * Legacy worker heartbeat message - REMOVED
 * @deprecated Use WorkerHeartbeatMessage instead
 */
// Legacy heartbeat message removed - use WorkerHeartbeatMessage instead

/**
 * Worker status message
 */
export interface WorkerStatusMessage extends BaseMessage {
  type: MessageType.WORKER_STATUS;
  worker_id: string;
  status?: string;
  capabilities?: Record<string, any>;
  timestamp?: number;
}

/**
 * Message to claim a job
 */
export interface ClaimJobMessage extends BaseMessage {
  type: MessageType.CLAIM_JOB;
  worker_id: string;
  job_id: string;
  claim_timeout?: number;
}

/**
 * Message indicating a job has been claimed
 */
export interface JobClaimedMessage extends BaseMessage {
  type: MessageType.JOB_CLAIMED;
  job_id: string;
  worker_id: string;
  success: boolean;
  job_data?: Record<string, any>;
  message?: string;
}

/**
 * Message to subscribe to job notifications - REMOVED
 * @deprecated Workers automatically receive job notifications
 */
// SubscribeJobNotificationsMessage removed - workers automatically receive job notifications

// Monitor Messages

/**
 * Stay alive message from monitor
 */
export interface StayAliveMessage extends BaseMessage {
  type: MessageType.STAY_ALIVE;
  monitor_id: string;
  timestamp: number;
}

/**
 * Stay alive response message to monitor
 */
export interface StayAliveResponseMessage extends BaseMessage {
  type: MessageType.STAY_ALIVE_RESPONSE;
  timestamp: number;
}

// Job Notification Messages

/**
 * Message indicating a job is available
 */
export interface JobAvailableMessage extends BaseMessage {
  type: MessageType.JOB_AVAILABLE;
  job_id: string;
  job_type: string;
  priority?: number;
  params_summary?: Record<string, any>;
}

/**
 * Connection established message
 */
export interface ConnectionEstablishedMessage extends BaseMessage {
  type: MessageType.CONNECTION_ESTABLISHED;
  message: string;
}

/**
 * Union type of all possible messages
 */
export type Message =
  | SubmitJobMessage
  | GetJobStatusMessage
  | RegisterWorkerMessage
  | UpdateJobProgressMessage
  | CompleteJobMessage
  | JobAcceptedMessage
  | JobStatusMessage
  | JobUpdateMessage
  | JobAssignedMessage
  | JobCompletedMessage
  | ErrorMessage
  | WorkerRegisteredMessage
  | SubscribeStatsMessage
  | GetStatsMessage
  | SubscribeJobMessage
  | WorkerHeartbeatMessage
  | WorkerStatusMessage
  | ClaimJobMessage
  | JobClaimedMessage
  | StayAliveMessage
  | StayAliveResponseMessage
  | JobAvailableMessage
  | ConnectionEstablishedMessage;

/**
 * Type guard to check if a message is of a specific type
 * @param message The message to check
 * @param type The message type to check for
 * @returns True if the message is of the specified type
 */
export function isMessageType<T extends Message>(
  message: Message,
  type: MessageType
): message is T {
  return message.type === type;
}
