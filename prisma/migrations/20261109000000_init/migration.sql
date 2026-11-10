-- CreateSchema
CREATE SCHEMA IF NOT EXISTS "public";

-- CreateEnum
CREATE TYPE "BatchStatus" AS ENUM ('PENDING_INGESTION', 'PARSING', 'VALIDATED', 'FAILED', 'COMPLETED');

-- CreateEnum
CREATE TYPE "IdempotencyStatus" AS ENUM ('STARTED', 'COMPLETED', 'FAILED');

-- CreateTable
CREATE TABLE "SampleBatch" (
    "id" UUID NOT NULL,
    "batchNumber" TEXT NOT NULL,
    "status" "BatchStatus" NOT NULL DEFAULT 'PENDING_INGESTION',
    "metadata" JSONB NOT NULL DEFAULT '{}',
    "version" INTEGER NOT NULL DEFAULT 1,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "SampleBatch_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "Sample" (
    "id" UUID NOT NULL,
    "batchId" UUID NOT NULL,
    "sampleCode" TEXT NOT NULL,
    "assayType" TEXT NOT NULL,
    "results" JSONB NOT NULL,
    "version" INTEGER NOT NULL DEFAULT 1,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "Sample_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "IngestionIdempotency" (
    "key" TEXT NOT NULL,
    "status" "IdempotencyStatus" NOT NULL,
    "responseData" JSONB,
    "lockedUntil" TIMESTAMP(3) NOT NULL,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "IngestionIdempotency_pkey" PRIMARY KEY ("key")
);

-- CreateTable
CREATE TABLE "S3IngestionLedger" (
    "s3Key" TEXT NOT NULL,
    "lastSequencer" TEXT NOT NULL,
    "eTag" TEXT NOT NULL,
    "status" TEXT NOT NULL,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "S3IngestionLedger_pkey" PRIMARY KEY ("s3Key")
);

-- CreateIndex
CREATE UNIQUE INDEX "SampleBatch_batchNumber_key" ON "SampleBatch"("batchNumber");

-- CreateIndex
CREATE INDEX "SampleBatch_status_idx" ON "SampleBatch"("status");

-- CreateIndex
CREATE UNIQUE INDEX "Sample_sampleCode_key" ON "Sample"("sampleCode");

-- CreateIndex
CREATE INDEX "Sample_batchId_idx" ON "Sample"("batchId");

-- CreateIndex
CREATE INDEX "IngestionIdempotency_status_lockedUntil_idx" ON "IngestionIdempotency"("status", "lockedUntil");

-- AddForeignKey
ALTER TABLE "Sample" ADD CONSTRAINT "Sample_batchId_fkey" FOREIGN KEY ("batchId") REFERENCES "SampleBatch"("id") ON DELETE CASCADE ON UPDATE CASCADE;
