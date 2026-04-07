CREATE EXTENSION IF NOT EXISTS "pgcrypto";

CREATE TABLE outbox (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    topic VARCHAR NOT NULL,
    key VARCHAR NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    published_at TIMESTAMP,
    processing_until TIMESTAMP
);

-- Prevents duplicate outbox entries for the same reservation+topic when
-- multiple service instances run the cleanup job concurrently.
CREATE UNIQUE INDEX outbox_pending_unique ON outbox (key, topic) WHERE published_at IS NULL;
