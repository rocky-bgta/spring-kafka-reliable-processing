CREATE TABLE business_events (
    event_id VARCHAR(255) PRIMARY KEY,
    payload TEXT NOT NULL,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_business_events_processed FOREIGN KEY (event_id)
        REFERENCES processed_events(event_id)
);
