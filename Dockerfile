# syntax=docker/dockerfile:1
FROM node:22-bookworm-slim

# Prisma's query engine needs OpenSSL at runtime.
RUN apt-get update -y && apt-get install -y --no-install-recommends openssl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Dependency layer first so source edits don't bust the npm cache.
COPY package.json package-lock.json ./
RUN npm ci

COPY prisma ./prisma
RUN npx prisma generate

COPY tsconfig.json vitest.config.ts ./
COPY src ./src
COPY tests ./tests

# Prototype has no long-running server; the default job applies migrations then runs
# typecheck + unit + Postgres integration tests (DATABASE_URL is injected by compose).
CMD ["sh", "-c", "npx prisma migrate deploy && npm run typecheck && npm test"]

