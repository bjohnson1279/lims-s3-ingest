import type { S3Notification } from "../ingestion/idempotency";

export interface DlqEntry {
  notification: S3Notification;
  idempotencyKey: string;
  batchNumber?: string;
  errorName: string;
  errorMessage: string;
  failedAt: string;
}

export interface DlqHandler {
  publish(entry: DlqEntry): Promise<void>;
}

/**
 * Mock DLQ. A production adapter would `SendMessage` to a dedicated SQS queue. The worker
 * wraps publish() in try/catch so a DLQ outage can never wedge or crash a consumer slot.
 */
export class MockDlqHandler implements DlqHandler {
  readonly entries: DlqEntry[] = [];
  async publish(entry: DlqEntry): Promise<void> {
    this.entries.push(entry);
  }
}
