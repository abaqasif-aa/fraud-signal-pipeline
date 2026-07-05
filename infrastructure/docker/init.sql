CREATE TABLE IF NOT EXISTS transactions_bronze (
    event_id          VARCHAR(36) PRIMARY KEY,
    event_timestamp   TIMESTAMPTZ NOT NULL,
    user_id           VARCHAR(20) NOT NULL,
    merchant_id       VARCHAR(20) NOT NULL,
    merchant_category VARCHAR(50),
    amount            NUMERIC(12, 2) NOT NULL,
    currency          CHAR(3) DEFAULT 'USD',
    country_code      CHAR(2),
    card_type         VARCHAR(20),
    is_online         BOOLEAN,
    device_type       VARCHAR(30),
    ip_address        VARCHAR(45),
    ingested_at       TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS fraud_signals (
    event_id           VARCHAR(36) PRIMARY KEY,
    event_timestamp    TIMESTAMPTZ NOT NULL,
    user_id            VARCHAR(20) NOT NULL,
    merchant_id        VARCHAR(20) NOT NULL,
    merchant_category  VARCHAR(50),
    amount             NUMERIC(12, 2) NOT NULL,
    currency           CHAR(3),
    country_code       CHAR(2),
    fraud_score        FLOAT NOT NULL,
    is_anomaly         BOOLEAN NOT NULL,
    anomaly_threshold  FLOAT NOT NULL,
    top_feature_1      VARCHAR(50),
    top_feature_2      VARCHAR(50),
    top_feature_3      VARCHAR(50),
    model_version      VARCHAR(30),
    processed_at       TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS dq_run_log (
    run_id              VARCHAR(36) PRIMARY KEY,
    run_timestamp       TIMESTAMPTZ DEFAULT NOW(),
    batch_id            VARCHAR(50),
    total_records       INTEGER,
    passed_records      INTEGER,
    failed_records      INTEGER,
    pass_rate           FLOAT,
    expectations_run    INTEGER,
    expectations_failed INTEGER,
    status              VARCHAR(20)
);

CREATE INDEX idx_fraud_signals_timestamp ON fraud_signals (event_timestamp DESC);
CREATE INDEX idx_fraud_signals_anomaly   ON fraud_signals (is_anomaly) WHERE is_anomaly = TRUE;
CREATE INDEX idx_fraud_signals_user      ON fraud_signals (user_id);
