import { Prisma, type PrismaClient } from "@prisma/client";

export interface LedgerEntry {
  s3Key: string;
  lastSequencer: string;
  eTag: string;
  status: string;
}

export interface LedgerStore {
  get(s3Key: string): Promise<LedgerEntry | null>;
  /**
   * Compare-and-set advance of the watermark. `expectedPrevious` is the sequencer the
   * caller observed (null = no row). Returns false if another worker moved it first,
   * which is harmless: the watermark only ever moves forward.
   */
  advance(entry: LedgerEntry, expectedPrevious: string | null): Promise<boolean>;
}

/** Ledger key: bucket-qualified so equal object keys in different buckets don't collide. */
export const ledgerKey = (bucket: string, key: string): string => `${bucket}:${key}`;

export class PrismaLedgerStore implements LedgerStore {
  constructor(private readonly prisma: PrismaClient) {}

  async get(s3Key: string): Promise<LedgerEntry | null> {
    return this.prisma.s3IngestionLedger.findUnique({
      where: { s3Key },
      select: { s3Key: true, lastSequencer: true, eTag: true, status: true },
    });
  }

  async advance(entry: LedgerEntry, expectedPrevious: string | null): Promise<boolean> {
    if (expectedPrevious === null) {
      try {
        await this.prisma.s3IngestionLedger.create({ data: entry });
        return true;
      } catch (err) {
        if (err instanceof Prisma.PrismaClientKnownRequestError && err.code === "P2002") return false;
        throw err;
      }
    }
    const res = await this.prisma.s3IngestionLedger.updateMany({
      where: { s3Key: entry.s3Key, lastSequencer: expectedPrevious },
      data: { lastSequencer: entry.lastSequencer, eTag: entry.eTag, status: entry.status },
    });
    return res.count === 1;
  }
}
