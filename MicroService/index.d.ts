// index.d.ts
import type { Logger } from "pino";

/** Options for creating the service */
export interface ServiceOptions {
  /** App name for logging (default: "microservice") */
  appName?: string;
  /** Service name for logging (default: "stream_consumer") */
  serviceName?: string;
  /** Container name/id for logging (default: random uuid) */
  containerName?: string;
  /** Path to YAML config (default: "./config.yml") */
  configPath?: string;
}

/** Context passed to each stream handler */
export interface HandlerContext {
  /** Child logger pre-bound with stream metadata */
  logger: Logger;
  /** The stream id: "default" => collection "events", otherwise "events_<id>" */
  streamId: string;
  /** Parsed YAML config (opaque to consumers) */
  config: unknown;
}

/** Shape of a stream handler */
export type StreamHandler = (
  doc: any,
  ctx: HandlerContext
) => void | Promise<void>;

/** Options for onStream (auto-pump mode) */
export interface OnStreamOptions {
  /** Max concurrent handler executions within a wave (default: 8) */
  concurrency?: number;
  /** Max queued handler tasks before backpressure (default: 2048) */
  maxQueue?: number;
  /**
   * When no stored offset (or resetOffset=true), how far back to scan (ms).
   * Capped internally at 7 days. Default: 10_000 ms.
   */
  initialBackfillMs?: number;
  /**
   * Ignore stored offset and start from initialBackfillMs window.
   * Default: false.
   */
  resetOffset?: boolean;
  /**
   * Optional per-stream mapper: receives the raw Mongo doc and returns
   * the payload delivered to the handler.
   */
  mapDoc?: (doc: any) => any | Promise<any>;
  /**
   * When a batch returns 0 docs, schedule a single retry after this delay (ms).
   * Default: 1000.
   */
  idleNudgeMs?: number;
  /**
   * Target batch size per pull. Default: internal BATCH_LIMIT (e.g., 2000).
   */
  batchSize?: number;
}

/** Control surface returned by onStream (auto-pump) */
export interface StreamControl {
  /** Pause the auto-pump; cancels any pending idle nudge. */
  pause(): void;
  /** Resume the auto-pump; immediately schedules work. */
  resume(): void;
  /** Change the per-pull batch size (must be > 0). */
  setBatchSize(n: number): void;
  /** Stop the stream, persist offsets, and wait for in-flight tasks to finish. */
  unsubscribe(): Promise<void>;
}

/** Service API returned by CreateService */
export interface ServiceApi {
  /**
   * Register a stream handler. Auto-starts pulling and continues:
   * - After each wave drains, immediately pulls the next batch.
   * - If a batch is empty, schedules a one-shot idle retry (idleNudgeMs).
   *
   * Returns a StreamControl for pause/resume/unsubscribe operations.
   */
  onStream(
    streamId: string,
    handler: StreamHandler,
    opts?: OnStreamOptions
  ): StreamControl;

  /** Flush offsets, drain queues, and close Mongo. Safe to call multiple times. */
  shutdown(): Promise<void>;

  /** Root logger instance */
  logger: Logger;

  /** Parsed YAML config (opaque to callers) */
  config: unknown;

  /** Get (or create) a store handle for a Mongo URL managed internally. */
  getStore(dbUrl: string): unknown;

  /** Get (or create) a pooled HTTP client for a base URL. */
  getService(serviceUrl: string): unknown;
}

/**
 * Bootstraps the microservice and returns the Service API.
 */
export function CreateService(opts?: ServiceOptions): Promise<ServiceApi>;
