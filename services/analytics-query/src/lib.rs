use std::path::{Path, PathBuf};

use datafusion::arrow::record_batch::RecordBatch;
use datafusion::prelude::{ParquetReadOptions, SessionContext};
use serde::{Deserialize, Serialize};

const APPROVED_DATASETS: &[&str] = &["provider_360", "decision_metrics", "geo_features"];

#[derive(Debug, Clone, Deserialize)]
pub struct AnalyticsRequest {
    pub tenant_id: String,
    pub dataset: String,
    pub limit: usize,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct QueryPlan {
    pub tenant_id: String,
    pub dataset: String,
    pub parquet_path: String,
    pub sql: String,
}

#[derive(Clone)]
pub struct AnalyticsEngine {
    lakehouse_root: PathBuf,
}

impl AnalyticsEngine {
    pub fn new(lakehouse_root: impl Into<PathBuf>) -> Self {
        Self {
            lakehouse_root: lakehouse_root.into(),
        }
    }

    pub fn plan(&self, request: &AnalyticsRequest) -> Result<QueryPlan, QueryError> {
        if !valid_tenant(&request.tenant_id) {
            return Err(QueryError::InvalidTenant);
        }
        if !APPROVED_DATASETS.contains(&request.dataset.as_str()) {
            return Err(QueryError::DatasetNotApproved);
        }
        if request.limit == 0 || request.limit > 1_000 {
            return Err(QueryError::InvalidLimit);
        }
        let path = self
            .lakehouse_root
            .join(&request.tenant_id)
            .join("gold")
            .join(&request.dataset)
            .join("data.parquet");
        let path = ensure_descendant(&self.lakehouse_root, &path)?;
        Ok(QueryPlan {
            tenant_id: request.tenant_id.clone(),
            dataset: request.dataset.clone(),
            parquet_path: path.display().to_string(),
            sql: format!("SELECT * FROM approved_dataset LIMIT {}", request.limit),
        })
    }

    pub async fn execute(
        &self,
        request: &AnalyticsRequest,
    ) -> Result<Vec<RecordBatch>, QueryError> {
        let plan = self.plan(request)?;
        let context = SessionContext::new();
        context
            .register_parquet(
                "approved_dataset",
                &plan.parquet_path,
                ParquetReadOptions::default(),
            )
            .await
            .map_err(QueryError::Execution)?;
        let dataframe = context
            .sql(&plan.sql)
            .await
            .map_err(QueryError::Execution)?;
        dataframe.collect().await.map_err(QueryError::Execution)
    }
}

#[derive(Debug)]
pub enum QueryError {
    InvalidTenant,
    DatasetNotApproved,
    InvalidLimit,
    PathEscape,
    Execution(datafusion::error::DataFusionError),
}

impl std::fmt::Display for QueryError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            QueryError::InvalidTenant => {
                write!(formatter, "invalid gateway-derived tenant context")
            }
            QueryError::DatasetNotApproved => {
                write!(formatter, "dataset is not approved for the analytics API")
            }
            QueryError::InvalidLimit => write!(formatter, "limit must be between 1 and 1000"),
            QueryError::PathEscape => write!(
                formatter,
                "resolved path escapes the configured lakehouse root"
            ),
            QueryError::Execution(error) => write!(formatter, "query execution failed: {error}"),
        }
    }
}

impl std::error::Error for QueryError {}

fn valid_tenant(tenant: &str) -> bool {
    tenant.len() >= 3
        && tenant.len() <= 63
        && tenant.chars().all(|character| {
            character.is_ascii_lowercase() || character.is_ascii_digit() || character == '-'
        })
        && tenant
            .chars()
            .next()
            .is_some_and(|character| character.is_ascii_lowercase())
}

fn ensure_descendant(root: &Path, candidate: &Path) -> Result<PathBuf, QueryError> {
    let root = root.components().collect::<Vec<_>>();
    let candidate = candidate.components().collect::<Vec<_>>();
    if candidate.starts_with(&root) {
        Ok(candidate.iter().collect())
    } else {
        Err(QueryError::PathEscape)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plan_is_tenant_scoped_and_read_only() {
        let engine = AnalyticsEngine::new("/lakehouse");
        let plan = engine
            .plan(&AnalyticsRequest {
                tenant_id: "demo-tenant".into(),
                dataset: "geo_features".into(),
                limit: 25,
            })
            .expect("approved plan");
        assert_eq!(
            plan.parquet_path,
            "/lakehouse/demo-tenant/gold/geo_features/data.parquet"
        );
        assert_eq!(plan.sql, "SELECT * FROM approved_dataset LIMIT 25");
    }

    #[test]
    fn plan_rejects_cross_tenant_path_and_unapproved_dataset() {
        let engine = AnalyticsEngine::new("/lakehouse");
        let error = engine
            .plan(&AnalyticsRequest {
                tenant_id: "../other".into(),
                dataset: "claims".into(),
                limit: 25,
            })
            .expect_err("tenant traversal must be rejected");
        assert!(matches!(error, QueryError::InvalidTenant));
        let error = engine
            .plan(&AnalyticsRequest {
                tenant_id: "demo-tenant".into(),
                dataset: "claims".into(),
                limit: 25,
            })
            .expect_err("dataset must be rejected");
        assert!(matches!(error, QueryError::DatasetNotApproved));
    }
}
