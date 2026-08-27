-- CRUSH healthcare streaming contracts.
-- Run in a Flink Application deployment with the Kafka and filesystem/S3 connectors.
-- All timestamps are event-time. No pattern below is a final adverse-action decision.

CREATE TABLE claim_events (
  tenant_id STRING,
  jurisdiction STRING,
  programme_id STRING,
  claim_id STRING,
  line_id STRING,
  provider_id STRING,
  beneficiary_token STRING,
  ordering_provider_id STRING,
  service_code STRING,
  diagnosis_codes ARRAY<STRING>,
  place_of_service STRING,
  service_lat DOUBLE,
  service_lon DOUBLE,
  amount DECIMAL(18,2),
  claim_status STRING,
  source_event_id STRING,
  source_version STRING,
  occurred_at TIMESTAMP(3),
  ingested_at TIMESTAMP(3),
  WATERMARK FOR occurred_at AS occurred_at - INTERVAL '10' MINUTE,
  PRIMARY KEY (tenant_id, jurisdiction, source_event_id) NOT ENFORCED
) WITH (
  'connector' = 'kafka',
  'topic' = 'healthcare.claim.received.v1',
  'properties.bootstrap.servers' = '${KAFKA_BROKERS}',
  'properties.group.id' = 'crush-claims-cep-v1',
  'scan.startup.mode' = 'group-offsets',
  'format' = 'json'
);

CREATE TABLE claim_anomaly_signals (
  tenant_id STRING,
  jurisdiction STRING,
  claim_id STRING,
  provider_id STRING,
  pattern_name STRING,
  first_event_at TIMESTAMP(3),
  last_event_at TIMESTAMP(3),
  event_count INT,
  amount_sum DECIMAL(18,2),
  source_event_ids ARRAY<STRING>,
  feature_version STRING,
  evidence_status STRING,
  emitted_at TIMESTAMP(3)
) WITH (
  'connector' = 'kafka',
  'topic' = 'healthcare.claim.anomaly-signal.v1',
  'properties.bootstrap.servers' = '${KAFKA_BROKERS}',
  'format' = 'json'
);

-- Pattern 1: rapid resubmission/reversal burst for one provider and claim line.
-- This is a prioritization signal; a legitimate corrected claim remains possible.
INSERT INTO claim_anomaly_signals
SELECT tenant_id, jurisdiction, claim_id, provider_id,
       'rapid_resubmission_reversal_burst',
       FIRST(event_time), LAST(event_time), COUNT(*), SUM(amount),
       ARRAY_AGG(source_event_id), 'cep-v1', 'needs_review', CURRENT_TIMESTAMP
FROM claim_events
MATCH_RECOGNIZE (
  PARTITION BY tenant_id, jurisdiction, provider_id, claim_id, line_id
  ORDER BY occurred_at
  MEASURES
    FIRST(A.occurred_at) AS event_time,
    LAST(R.occurred_at) AS last_time,
    COUNT(*) AS event_count,
    SUM(R.amount) AS amount_sum,
    FIRST(A.source_event_id) AS first_id
  PATTERN (A R{2,}?) WITHIN INTERVAL '24' HOUR
  DEFINE
    A AS A.claim_status IN ('submitted', 'accepted'),
    R AS R.claim_status IN ('reversed', 'rejected', 'resubmitted')
) MR;

-- Pattern 2: provider-level high-velocity claims with unusual beneficiary fan-out.
-- The exact thresholds are policy parameters, not model output.
INSERT INTO claim_anomaly_signals
SELECT tenant_id, jurisdiction, CAST(NULL AS STRING), provider_id,
       'provider_beneficiary_velocity',
       MIN(occurred_at), MAX(occurred_at), COUNT(*), SUM(amount),
       ARRAY_AGG(source_event_id), 'cep-v1', 'needs_review', CURRENT_TIMESTAMP
FROM claim_events
MATCH_RECOGNIZE (
  PARTITION BY tenant_id, jurisdiction, provider_id
  ORDER BY occurred_at
  MEASURES
    FIRST(C.occurred_at) AS first_time,
    LAST(C.occurred_at) AS last_time,
    COUNT(C.*) AS event_count,
    SUM(C.amount) AS amount_sum,
    FIRST(C.source_event_id) AS first_id
  PATTERN (C{25,}) WITHIN INTERVAL '6' HOUR
  DEFINE
    C AS C.beneficiary_token IS NOT NULL
) MR
GROUP BY tenant_id, jurisdiction, provider_id;

-- Pattern 3: repeated same-place / inconsistent-geography claims.
-- Spatial distance is computed by Sedona/Spark; CEP keeps the burst/time logic.
INSERT INTO claim_anomaly_signals
SELECT tenant_id, jurisdiction, CAST(NULL AS STRING), provider_id,
       'same_provider_geographic_burst',
       MIN(occurred_at), MAX(occurred_at), COUNT(*), SUM(amount),
       ARRAY_AGG(source_event_id), 'cep-v1', 'needs_geo_review', CURRENT_TIMESTAMP
FROM claim_events
MATCH_RECOGNIZE (
  PARTITION BY tenant_id, jurisdiction, provider_id
  ORDER BY occurred_at
  MEASURES
    FIRST(G.occurred_at) AS first_time,
    LAST(G.occurred_at) AS last_time,
    COUNT(G.*) AS event_count,
    SUM(G.amount) AS amount_sum,
    FIRST(G.source_event_id) AS first_id
  PATTERN (G{10,}) WITHIN INTERVAL '2' HOUR
  DEFINE
    G AS G.service_lat IS NOT NULL AND G.service_lon IS NOT NULL
) MR
GROUP BY tenant_id, jurisdiction, provider_id;

-- Operational requirements:
-- 1. Keep Kafka key = tenant_id|jurisdiction|provider_id|claim_id where possible.
-- 2. Configure checkpointing exactly-once and a durable checkpoint/savepoint URI.
-- 3. Route late events to a quarantine topic and replay them through a correction job.
-- 4. Do not publish any row from this file directly to a payment, suspension, or denial API.
