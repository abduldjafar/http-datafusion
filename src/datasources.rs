use std::any::Any;
use std::sync::{Arc, Mutex};
use async_trait::async_trait;
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::json::reader::infer_json_schema_from_iterator;
use datafusion::catalog::{Session, TableProvider};
use datafusion::physical_plan::ExecutionPlan;
use datafusion_expr::{Expr, TableType};
use serde_json::Value;
use datafusion::error::Result;
use reqwest::Client;
use std::io::BufReader;
use std::io::Cursor;
use std::fmt::{Debug, Formatter};

use crate::execution::HttpExec;





#[derive(Clone)]
pub struct HttpDataSource {
    pub inner: Arc<Mutex<HttpDataSourceInner>>,
}

pub struct HttpDataSourceInner {
    pub data: Vec<Value>,
    pub first_data: Value,
    pub schema: Schema,
}


impl Debug for HttpDataSource {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("HttpDataSource")
    }
}

impl HttpDataSource {
    pub async fn create_physical_plan(
        &self,
        projections: Option<&Vec<usize>>,
        schema: SchemaRef,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(HttpExec::new(projections, schema, self.clone())))
    }

    pub async fn populate_data(
        &self,
        main_url: String,
        start_page: Option<String>,
        method: String,
    ) -> Result<()> {
        let mut temp_data: Vec<Value> = vec![];

        // Ensure we have a starting page for pagination
        if let Some(mut current_page) = start_page.clone() {
            loop {
                // Build the URL for the current page
                let url = format!("{}?page={}", main_url, current_page);
                println!("processing {}", url);
                let cloned_method = method.clone();
                // Fetch data using `data_extraction`
                let data = self
                    .data_extraction(&url, cloned_method)
                    .await
                    .map_err(|e| {
                        datafusion::error::DataFusionError::Execution(format!(
                            "Error fetching data from page {}: {}",
                            current_page, e
                        ))
                    })?;

                // Check if the fetched data is empty
                if data == Value::Null {
                    println!("No more data to fetch. Ending pagination.");
                    break;
                }

                if data.is_array() {
                    let data_array = data.as_array().unwrap();
                    // Extend inner.data with the items in the array
                    if data_array.len() > 0 {
                        temp_data.extend(data_array.clone());
                    }
                } else {
                    // If it's not an array (and not empty), store the data as a single item
                    if !data.is_null() {
                        temp_data.push(data);
                    }
                }

                // Convert `current_page` to an integer, increment it, and update the string
                let next_page = current_page.parse::<i32>().unwrap_or(0) + 1;
                current_page = next_page.to_string();
            }
        } else {
            let url = format!("{}", main_url);

            // Fetch data using `data_extraction`
            let data = self.data_extraction(&url, method).await.map_err(|e| {
                datafusion::error::DataFusionError::Execution(format!(
                    "Error fetching data from page : {}",
                    e
                ))
            })?;

            if data.is_array() {
                let data_array = data.as_array().unwrap();
                // Extend inner.data with the items in the array
                if data_array.len() > 0 {
                    temp_data.extend(data_array.clone());
                }
            } else {
                // If it's not an array (and not empty), store the data as a single item
                if !data.is_null() {
                    temp_data.push(data);
                }
            }
        }
        let mut inner = self.inner.lock().unwrap();
        inner.data = temp_data;

        inner.first_data = inner.data.first().cloned().unwrap();
        inner.schema = self._schema(inner.first_data.clone());

        Ok(())
    }

    async fn data_extraction(&self, url: &str, method: String) -> Result<Value> {
        let client = Client::new(); // Create a new HTTP client

        // Determine the HTTP method
        let method = match method.as_str() {
            "GET" => reqwest::Method::GET,
            "POST" => reqwest::Method::POST,
            _ => {
                return Err(datafusion::error::DataFusionError::Execution(format!(
                    "No Method Avaliable"
                )))
            }
        };

        // Create the request
        let req = client.request(method, url);

        let req = req.build().map_err(|e| {
            datafusion::error::DataFusionError::Execution(format!(
                "Invalid request: {}",
                e.to_string()
            ))
        })?;

        let response = client.execute(req).await.map_err(|e| {
            datafusion::error::DataFusionError::Execution(format!(
                "Request execution failed: {}",
                e.to_string()
            ))
        })?;

        // Check if the response is empty
        if response.status().is_success() {
            let json = response.json::<Value>().await.map_err(|e| {
                datafusion::error::DataFusionError::Execution(format!(
                    "Failed to parse JSON: {}",
                    e.to_string()
                ))
            })?;
            // If JSON is empty (null or empty object), return null
            if json.is_null() {
                return Ok(Value::Null); // Return null if empty
            }

            Ok(json)
        } else {
            Err(datafusion::error::DataFusionError::Execution(format!(
                "HTTP request failed with status code: {}",
                response.status()
            )))
        }
    }

    fn _schema(&self, first_item: Value) -> Schema {
        let json_data = serde_json::to_string(&first_item).unwrap_or_else(|_| {
            panic!("Failed to serialize first item to JSON");
        });

        if json_data.is_empty() {
            panic!("No data available to infer schema");
        }

        let reader = BufReader::new(Cursor::new(json_data));
        let deserializer = serde_json::Deserializer::from_reader(reader);
        let value_iter = deserializer
            .into_iter::<Value>()
            .map(|v| v.map_err(|e| ArrowError::ExternalError(Box::new(e))));

        let inferred_schema = infer_json_schema_from_iterator(Box::new(value_iter))
            .unwrap_or_else(|_| panic!("Failed to infer schema from JSON data"));

        inferred_schema
    }
}

impl Default for HttpDataSource {
    fn default() -> Self {
        HttpDataSource {
            inner: Arc::new(Mutex::new(HttpDataSourceInner {
                data: Default::default(),
                first_data: Default::default(),
                schema: Schema::empty(),
            })),
        }
    }
}
#[async_trait]
impl TableProvider for HttpDataSource {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        let schema = self.inner.lock().unwrap().schema.clone();

        Arc::new(schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        // filters and limit can be used here to inject some push-down operations if needed
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        return self.create_physical_plan(projection, self.schema()).await;
    }
}