use datafusion::prelude::SessionContext;
use std::sync::Arc;
use crate::datasources::HttpDataSource;

pub async fn url(ctx: SessionContext,url: &str, method: &str, table_name: &str,start_page:Option<String>) -> datafusion::error::Result<(SessionContext)> {
    // Create session context
    
    // Create HTTP data source
    let db = HttpDataSource::default();
    
    // Populate data from API
    println!("📡 Fetching data from API...");
    db.populate_data(
        url, 
        start_page, 
        method
    ).await?;
    
    // Register the data source as a table
    ctx.register_table(table_name, Arc::new(db))?;

    Ok(ctx)
}

