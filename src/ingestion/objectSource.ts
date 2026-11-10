import type { Readable } from "node:stream";
import type { S3Notification } from "./idempotency";

/** Port for fetching the object body as a stream (S3 GetObject adapter lives outside this prototype). */
export interface ObjectSource {
  getObjectStream(notification: S3Notification): Promise<Readable>;
}
