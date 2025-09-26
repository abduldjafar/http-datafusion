//! HTTP DataSource for DataFusion
//! 
//! Provides HTTP-based data source capabilities for Apache DataFusion,
//! enabling SQL queries over HTTP APIs with automatic pagination support.

use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::io::{BufReader, Cursor};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use reqwest::{Client, ClientBuilder};
use serde_json::Value;

use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::json::reader::infer_json_schema_from_iterator;
use datafusion::catalog::{Session, TableProvider};
use crate::error::{Error, Result};
use datafusion::physical_plan::ExecutionPlan;
use datafusion_expr::{Expr, TableType};

use crate::execution::HttpExec;
use crate::model::Source;

// ============================================================================
// Core Data Structures
// ============================================================================

/// Main HTTP DataSource that implements DataFusion's TableProvider trait
#[derive(Clone)]
pub struct HttpDataSource {
    /// Thread-safe inner data storage
    pub inner: Arc<Mutex<HttpDataSourceInner>>,
}

/// Inner data structure containing the actual data and metadata
pub struct HttpDataSourceInner {
    /// Vector of JSON values fetched from HTTP endpoints
    pub data: Vec<Value>,
    /// First data item used for schema inference
    pub first_data: Value,
    /// Inferred Arrow schema from the data
    pub schema: Schema,
    /// Source configuration
    pub source: Source,
}

// ============================================================================
// Debug Implementation
// ============================================================================

impl Debug for HttpDataSource {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("HttpDataSource")
    }
}

// ============================================================================
// Default Implementation
// ============================================================================

impl Default for HttpDataSource {
    fn default() -> Self {
        HttpDataSource {
            inner: Arc::new(Mutex::new(HttpDataSourceInner {
                data: Default::default(),
                first_data: Default::default(),
                schema: Schema::empty(),
                source: Source::default(),
            })),
        }
    }
}

// ============================================================================
// Core Implementation
// ============================================================================

impl HttpDataSource {
    /// Creates a physical execution plan for DataFusion query execution
    /// 
    /// # Arguments
    /// * `projections` - Optional column projections to optimize data access
    /// * `schema` - Arrow schema reference for the data
    /// 
    /// # Returns
    /// * `Result<Arc<dyn ExecutionPlan>>` - Physical execution plan
    pub async fn create_physical_plan(
        &self,
        projections: Option<&Vec<usize>>,
        schema: SchemaRef,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(HttpExec::new(projections, schema, self.clone())))
    }

    /// Main entry point for populating data from HTTP endpoints
    /// 
    /// Supports both single endpoint fetching and automatic pagination.
    /// When `start_page` is provided, the function will automatically
    /// paginate through all available pages until no more data is found.
    /// 
    /// # Arguments
    /// * `main_url` - Base URL for the HTTP endpoint
    /// * `start_page` - Optional starting page for pagination
    /// * `method` - HTTP method to use (GET, POST, etc.)
    /// 
    /// # Returns
    /// * `Result<()>` - Success or error result
    pub async fn populate_data(
        &self,
        main_url: &str,
        start_page: Option<String>,
        method: &str,
    ) -> Result<()> {
        let mut temp_data: Vec<Value> = vec![];

        // Handle paginated data fetching
        if let Some(mut current_page) = start_page.clone() {
            loop {
                // Build the URL for the current page
                let url = format!("{}?page={}", main_url, current_page);
                println!("processing {}", url);
                
               
                
                // Fetch data using `data_extraction`
                let data = self
                    .data_extraction(&url, method)
                    .await
                    .map_err(|e| {
                        datafusion::error::DataFusionError::Execution(format!(
                            "Error fetching data from page {}: {}",
                            current_page, e
                        ))
                    })?;

                // Check if the fetched data is empty (end of pagination)
                if data == Value::Null {
                    println!("No more data to fetch. Ending pagination.");
                    break;
                }

                // Process the fetched data based on its type
                if data.is_array() {
                    let data_array = data.as_array().unwrap();
                    // Extend temp_data with the items in the array
                    if !data_array.is_empty() {
                        temp_data.extend(data_array.clone());
                    }
                } else {
                    // If it's not an array (and not empty), store the data as a single item
                    if !data.is_null() {
                        temp_data.push(data);
                    }
                }

                // Convert current_page to an integer, increment it, and update the string
                let next_page = current_page.parse::<i32>().unwrap_or(0) + 1;
                current_page = next_page.to_string();
            }
        } else {
            // Handle single endpoint fetching (no pagination)
            let url = format!("{}", main_url);

            // Fetch data using `data_extraction`
            let data = self
                .data_extraction(&url, method)
                .await
                .map_err(|e| {
                    datafusion::error::DataFusionError::Execution(format!(
                        "Error fetching data from page: {}",
                        e
                    ))
                })?;

            // Process the fetched data based on its type
            if data.is_array() {
                let data_array = data.as_array().unwrap();
                // Extend temp_data with the items in the array
                if !data_array.is_empty() {
                    temp_data.extend(data_array.clone());
                }
            } else {
                // If it's not an array (and not empty), store the data as a single item
                if !data.is_null() {
                    temp_data.push(data);
                }
            }
        }

        // Update the inner data structure with fetched data
        let mut inner = self.inner.lock().unwrap();
        inner.data = temp_data;
        inner.first_data = inner.data.first().cloned().unwrap();
        inner.schema = self._schema(inner.first_data.clone());

        Ok(())
    }

    /// Performs the actual HTTP request and data extraction
    /// 
    /// Handles HTTP method selection, request building, execution,
    /// and JSON response parsing with comprehensive error handling.
    /// 
    /// # Arguments
    /// * `url` - Complete URL to fetch data from
    /// * `method` - HTTP method as string (GET, POST, etc.)
    /// 
    /// # Returns
    /// * `Result<Value>` - Parsed JSON response or error
    async fn data_extraction(&self, url: &str, method: &str) -> Result<Value> {
        // Create a new HTTP client
        let client = Client::new();

        // Determine the HTTP method
        let method = match method {
            "GET" => reqwest::Method::GET,
            "POST" => reqwest::Method::POST,
            _ => {
                return Err(Error::ReqwestError("No Method Available".to_string()))
            }
        };

        // Create and build the HTTP request
        let req = client
            .request(method, url)
            .build()
            .map_err(|e| {
                datafusion::error::DataFusionError::Execution(format!(
                    "Invalid request: {}",
                    e.to_string()
                ))
            })?;

        // Execute the HTTP request
        let response = client
            .execute(req)
            .await
            .map_err(|e| {
                datafusion::error::DataFusionError::Execution(format!(
                    "Request execution failed: {}",
                    e.to_string()
                ))
            })?;

        // Process the response
        if response.status().is_success() {
            let json = response
                .json::<Value>()
                .await
                .map_err(|e| {
                    datafusion::error::DataFusionError::Execution(format!(
                        "Failed to parse JSON: {}",
                        e.to_string()
                    ))
                })?;

            // If JSON is empty (null or empty object), return null
            if json.is_null() {
                return Ok(Value::Null);
            }

            Ok(json)
        } else {
            Err(Error::ReqwestError(format!("HTTP request failed with status code: {}", response.status())))
        }
    }

    /// Infers Arrow schema from the first data item
    /// 
    /// Uses Arrow's built-in JSON schema inference to automatically
    /// detect field types and structure from the first JSON object.
    /// 
    /// # Arguments
    /// * `first_item` - First JSON value to infer schema from
    /// 
    /// # Returns
    /// * `Schema` - Inferred Arrow schema
    /// 
    /// # Panics
    /// * If JSON serialization fails
    /// * If no data is available for schema inference
    /// * If schema inference from JSON data fails
    
    async fn data_extraction_from_source(&self, source: &Source) -> Result<()> {
        // Create a new HTTP client
        let  client = ClientBuilder::new().build()?;
       

        let url = &source.url;
        let method = source.method.clone().unwrap_or("GET".to_string());

        // Convert method String to reqwest::Method
        let method = method.parse::<reqwest::Method>().map_err(|e| {
            Error::ReqwestError(format!("Invalid HTTP method: {}", e))
        })?;

        let mut response_builder = client.request(method, url);

        if let Some(pagination) = &source.pagination {
            let start_page = pagination.start_page.unwrap_or(1);
            let end_page = pagination.end_page.unwrap_or(10);
            let page_size = pagination.page_size.unwrap_or(10);
            let page_param = pagination.page_param.clone().unwrap_or("page".to_string());
            let page_size_param = pagination.page_size_param.clone().unwrap_or("page_size".to_string());
            let page_size_default = pagination.page_size_default.unwrap_or(10);

            response_builder = response_builder.query(&[(page_param, start_page,), (page_size_param, page_size)]);

        };

        Ok(())

        
    }

    fn _schema(&self, first_item: Value) -> Schema {
        // Serialize the first item to JSON string
        let json_data = serde_json::to_string(&first_item).unwrap_or_else(|_| {
            panic!("Failed to serialize first item to JSON");
        });

        // Ensure we have data to work with
        if json_data.is_empty() {
            panic!("No data available to infer schema");
        }

        // Create a buffered reader from the JSON data
        let reader = BufReader::new(Cursor::new(json_data));
        let deserializer = serde_json::Deserializer::from_reader(reader);
        
        // Create an iterator that converts JSON values to Arrow-compatible format
        let value_iter = deserializer
            .into_iter::<Value>()
            .map(|v| v.map_err(|e| ArrowError::ExternalError(Box::new(e))));

        // Infer the Arrow schema from the JSON data
        let inferred_schema = infer_json_schema_from_iterator(Box::new(value_iter))
            .unwrap_or_else(|_| panic!("Failed to infer schema from JSON data"));

        inferred_schema
    }
}

// ============================================================================
// DataFusion TableProvider Implementation
// ============================================================================

#[async_trait]
impl TableProvider for HttpDataSource {
    /// Returns this instance as Any for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Returns the Arrow schema for this table
    fn schema(&self) -> SchemaRef {
        let schema = self.inner.lock().unwrap().schema.clone();
        Arc::new(schema)
    }

    /// Returns the table type (Base table in this case)
    fn table_type(&self) -> TableType {
        TableType::Base
    }

    /// Creates a scan plan for query execution
    /// 
    /// This method is called by DataFusion when executing queries.
    /// It creates a physical execution plan that can read and process
    /// the HTTP data according to the query requirements.
    /// 
    /// # Arguments
    /// * `_state` - Session state (unused in this implementation)
    /// * `projection` - Optional column projections for optimization
    /// * `_filters` - Query filters (can be used for push-down operations)
    /// * `_limit` - Query limit (can be used for push-down operations)
    /// 
    /// # Returns
    /// * `Result<Arc<dyn ExecutionPlan>>` - Physical execution plan
    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        // filters and limit can be used here to inject some push-down operations if needed
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        self.create_physical_plan(projection, self.schema()).await
    }
}