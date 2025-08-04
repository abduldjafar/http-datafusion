//! HTTP DataFusion Example Application
//! 
//! Demonstrates querying HTTP APIs using SQL through DataFusion's HTTP data source.
//! This example fetches data from JSONPlaceholder API and executes SQL queries
//! against the HTTP data as if it were a traditional database table.

use std::fs;

use datafusion::prelude::SessionContext;
use tokio;
use http_datafusion::{dataframe, model::Config};
use http_datafusion::error::Result;

/// Main application entry point
/// 
/// Sets up an HTTP data source, registers it as a queryable table,
/// and demonstrates SQL query execution against HTTP API data.
/// 
/// # Returns
/// * `datafusion::error::Result<()>` - Success or DataFusion error
#[tokio::main]
async fn main() -> Result<()> {

    // Read the YAML file
    let yaml_content = fs::read_to_string("config.yaml")?;

    // Parse YAML into our struct, mapping errors to DataFusionError
    let config: Config = serde_yaml::from_str(&yaml_content)?;

    // ========================================================================
    // HTTP Data Source Configuration
    // ========================================================================

    let mut ctx = SessionContext::new();

    for source in config.sources {
        let api_url = source.url;
        let http_method = source.method.unwrap_or("GET".to_string());
        let table_name = source.name;

        ctx = dataframe::url(ctx, &api_url, &http_method, &table_name,None).await?;
        if let Some(sql) = source.sql {
            let df = ctx.sql(&sql).await?;
            df.show().await?;
        }
    }

    Ok(())
}