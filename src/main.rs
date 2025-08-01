//! HTTP DataFusion Example Application
//! 
//! Demonstrates querying HTTP APIs using SQL through DataFusion's HTTP data source.
//! This example fetches data from JSONPlaceholder API and executes SQL queries
//! against the HTTP data as if it were a traditional database table.

use tokio;
use http_datafusion::dataframe;

/// Main application entry point
/// 
/// Sets up an HTTP data source, registers it as a queryable table,
/// and demonstrates SQL query execution against HTTP API data.
/// 
/// # Returns
/// * `datafusion::error::Result<()>` - Success or DataFusion error
#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    // ========================================================================
    // HTTP Data Source Configuration
    // ========================================================================
    
    // API endpoint for sample JSON data
    let api_url = "https://jsonplaceholder.typicode.com/posts";
    
    // HTTP method for the request
    let http_method = "GET";
    
    // Table name for SQL queries
    let table_name = "posts";
    
    // No pagination (None = single endpoint fetch)
    let pagination = None;

    // ========================================================================
    // DataFusion Context Setup
    // ========================================================================
    
    // Create DataFusion context with HTTP data source
    // This fetches data from the API and registers it as a queryable table
    let ctx = dataframe::url(
        api_url,
        http_method,
        table_name,
        pagination,
    ).await?;

    // ========================================================================
    // SQL Query Execution
    // ========================================================================
    
    // Execute SQL query against the HTTP data source
    // The API data is now queryable as a regular SQL table
    let df = ctx
        .sql("SELECT * FROM posts LIMIT 5")
        .await?;

    // ========================================================================
    // Results Display
    // ========================================================================
    
    // Display query results in a formatted table
    df.show().await?;

    Ok(())
}