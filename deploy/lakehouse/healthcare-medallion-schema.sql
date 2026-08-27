-- Delta Lake medallion schema for CRUSH healthcare integrity.
-- Execute with Spark SQL and Delta Lake enabled. Replace LOCATION roots per environment.

CREATE TABLE IF NOT EXISTS crush_bronze_source_envelopes (
  tenant_id STRING NOT NULL,
  jurisdiction STRING NOT NULL,
  programme_id STRING,
  source_system STRING NOT NULL,
  source_event_id STRING NOT NULL,
  event_type STRING NOT NULL,
  payload_ref STRING NOT NULL,
  payload_sha256 STRING NOT NULL,
  purpose_of_use STRING NOT NULL,
  data_class STRING NOT NULL,
  occurred_at TIMESTAMP,
  ingested_at TIMESTAMP NOT NULL,
  schema_version STRING NOT NULL,
  schema_fingerprint STRING NOT NULL,
  validation_status STRING NOT NULL,
  validation_errors ARRAY<STRING>,
  unexpected_fields ARRAY<STRING>,
  ingestion_run_id STRING NOT NULL,
  legal_hold BOOLEAN,
  CONSTRAINT bronze_source_event_unique EXPECT (source_event_id IS NOT NULL) ON VIOLATION DROP ROW
) USING DELTA
PARTITIONED BY (jurisdiction, date(ingested_at))
LOCATION '${DELTA_ROOT}/bronze/source_envelopes';

CREATE TABLE IF NOT EXISTS crush_silver_claim_lines (
  tenant_id STRING NOT NULL,
  jurisdiction STRING NOT NULL,
  programme_id STRING NOT NULL,
  claim_id STRING NOT NULL,
  line_id STRING NOT NULL,
  provider_id STRING,
  beneficiary_token STRING,
  ordering_provider_id STRING,
  supplier_id STRING,
  plan_id STRING,
  service_code STRING,
  diagnosis_codes ARRAY<STRING>,
  place_of_service STRING,
  service_start DATE,
  service_end DATE,
  service_lat DOUBLE,
  service_lon DOUBLE,
  submitted_amount DECIMAL(18,2),
  paid_amount DECIMAL(18,2),
  claim_status STRING,
  source_event_id STRING NOT NULL,
  source_system STRING NOT NULL,
  source_version STRING NOT NULL,
  occurred_at TIMESTAMP NOT NULL,
  normalized_at TIMESTAMP NOT NULL,
  record_sha256 STRING NOT NULL,
  quality_status STRING NOT NULL,
  quality_errors ARRAY<STRING>
) USING DELTA
PARTITIONED BY (jurisdiction, service_start)
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'delta.columnMapping.mode' = 'name')
LOCATION '${DELTA_ROOT}/silver/claim_lines';

CREATE TABLE IF NOT EXISTS crush_silver_entity_links (
  tenant_id STRING NOT NULL,
  jurisdiction STRING NOT NULL,
  left_entity_type STRING NOT NULL,
  left_entity_id STRING NOT NULL,
  right_entity_type STRING NOT NULL,
  right_entity_id STRING NOT NULL,
  relationship_type STRING NOT NULL,
  match_method STRING NOT NULL,
  match_score DOUBLE,
  abstain_reason STRING,
  positive_evidence ARRAY<STRING>,
  negative_evidence ARRAY<STRING>,
  source_event_ids ARRAY<STRING>,
  resolver_version STRING NOT NULL,
  valid_from TIMESTAMP NOT NULL,
  valid_to TIMESTAMP,
  created_at TIMESTAMP NOT NULL,
  record_sha256 STRING NOT NULL
) USING DELTA
PARTITIONED BY (jurisdiction, relationship_type)
LOCATION '${DELTA_ROOT}/silver/entity_links';

CREATE TABLE IF NOT EXISTS crush_silver_graph_edges (
  tenant_id STRING NOT NULL,
  jurisdiction STRING NOT NULL,
  snapshot_id STRING NOT NULL,
  src_type STRING NOT NULL,
  src_id STRING NOT NULL,
  edge_type STRING NOT NULL,
  dst_type STRING NOT NULL,
  dst_id STRING NOT NULL,
  edge_weight DOUBLE,
  edge_confidence DOUBLE,
  source_event_ids ARRAY<STRING>,
  valid_from TIMESTAMP NOT NULL,
  valid_to TIMESTAMP,
  resolver_version STRING NOT NULL,
  geometry_version STRING,
  created_at TIMESTAMP NOT NULL
) USING DELTA
PARTITIONED BY (jurisdiction, snapshot_id)
LOCATION '${DELTA_ROOT}/silver/graph_edges';

CREATE TABLE IF NOT EXISTS crush_gold_claim_features (
  tenant_id STRING NOT NULL,
  jurisdiction STRING NOT NULL,
  as_of_time TIMESTAMP NOT NULL,
  claim_id STRING NOT NULL,
  line_id STRING NOT NULL,
  provider_id STRING,
  feature_set_version STRING NOT NULL,
  source_snapshot_id STRING NOT NULL,
  claim_count_6h BIGINT,
  claim_count_24h BIGINT,
  beneficiary_count_24h BIGINT,
  reversal_count_24h BIGINT,
  provider_peer_residual DOUBLE,
  provider_graph_degree BIGINT,
  suspicious_component_size BIGINT,
  provider_beneficiary_distance_km DOUBLE,
  impossible_travel_flag BOOLEAN,
  geo_density_z DOUBLE,
  missingness_flags ARRAY<STRING>,
  freshness_seconds BIGINT,
  features_sha256 STRING NOT NULL
) USING DELTA
PARTITIONED BY (jurisdiction, date(as_of_time))
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
LOCATION '${DELTA_ROOT}/gold/claim_features';

CREATE TABLE IF NOT EXISTS crush_gold_model_assessments (
  tenant_id STRING NOT NULL,
  jurisdiction STRING NOT NULL,
  as_of_time TIMESTAMP NOT NULL,
  subject_type STRING NOT NULL,
  subject_id STRING NOT NULL,
  assessment_id STRING NOT NULL,
  model_name STRING NOT NULL,
  model_version STRING NOT NULL,
  graph_snapshot_id STRING,
  feature_set_version STRING NOT NULL,
  score DOUBLE,
  lower_bound DOUBLE,
  upper_bound DOUBLE,
  calibration_version STRING,
  assessment_type STRING NOT NULL,
  reason_codes ARRAY<STRING>,
  evidence_refs ARRAY<STRING>,
  abstained BOOLEAN NOT NULL,
  abstain_reason STRING,
  created_at TIMESTAMP NOT NULL
) USING DELTA
PARTITIONED BY (jurisdiction, date(as_of_time))
LOCATION '${DELTA_ROOT}/gold/model_assessments';

CREATE TABLE IF NOT EXISTS crush_gold_evidence_packs (
  tenant_id STRING NOT NULL,
  jurisdiction STRING NOT NULL,
  case_id STRING NOT NULL,
  evidence_pack_id STRING NOT NULL,
  claim_ids ARRAY<STRING>,
  source_span_refs ARRAY<STRUCT<document_id:STRING,page:INT,start_offset:INT,end_offset:INT,sha256:STRING>>,
  graph_subgraph_ref STRING,
  retrieval_refs ARRAY<STRUCT<document_id:STRING,version:STRING,locator:STRING>>,
  ocr_model_version STRING,
  nlp_model_version STRING,
  llm_model_version STRING,
  prompt_policy_version STRING,
  gate_results MAP<STRING,STRING>,
  reviewer_required BOOLEAN NOT NULL,
  created_at TIMESTAMP NOT NULL,
  content_sha256 STRING NOT NULL
) USING DELTA
PARTITIONED BY (jurisdiction, date(created_at))
LOCATION '${DELTA_ROOT}/gold/evidence_packs';

-- Silver is normalized but lineage-preserving. Gold is point-in-time and purpose-limited.
-- Never expose bronze payload_ref contents to model services without an explicit data-use grant.
-- Use Delta time travel/change data feed for replay and reproducibility; do not mutate evidence packs in place.
