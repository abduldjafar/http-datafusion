//! HTTP Execution Plan for DataFusion
//! 
//! Provides physical execution capabilities for HTTP-based data sources,
//! converting JSON data from HTTP APIs into Arrow RecordBatches for
//! efficient query processing within the DataFusion engine.

use std::{any::Any, fmt, io::Cursor, sync::Arc};

use datafusion::error::Result;
use datafusion::{
    arrow::{datatypes::SchemaRef, json::ReaderBuilder},
    common::project_schema,
    execution::{SendableRecordBatchStream, TaskContext},
    physical_expr::EquivalenceProperties,
    physical_plan::{
        memory::MemoryStream, DisplayAs, DisplayFormatType, ExecutionMode, ExecutionPlan,
        Partitioning, PlanProperties,
    },
};

use crate::datasources::HttpDataSource;

// ============================================================================
// HTTP Execution Plan Structure
// ============================================================================

/// Physical execution plan for HTTP-based data sources
/// 
/// This struct implements DataFusion's ExecutionPlan trait to provide
/// efficient execution of queries against HTTP API data. It handles
/// schema projection, data conversion from JSON to Arrow format,
/// and stream-based result delivery.
#[derive(Debug, Clone)]
pub struct HttpExec {
    /// Reference to the HTTP data source containing the fetched data
    db: HttpDataSource,
    /// Schema after applying column projections for query optimization
    projected_schema: SchemaRef,
    /// Cached plan properties for performance optimization
    cache: PlanProperties,
}

// ============================================================================
// Core Implementation
// ============================================================================

impl HttpExec {
    /// Creates a new HTTP execution plan instance
    /// 
    /// Applies column projections to optimize data access and pre-computes
    /// plan properties for efficient query execution.
    /// 
    /// # Arguments
    /// * `projections` - Optional column indices to project (for SELECT optimization)
    /// * `schema` - Full Arrow schema of the data source
    /// * `db` - HTTP data source containing the actual data
    /// 
    /// # Returns
    /// * `Self` - New HttpExec instance ready for execution
    pub fn new(
        projections: Option<&Vec<usize>>,
        schema: SchemaRef,
        db: HttpDataSource,
    ) -> Self {
        // Apply column projections to the schema for optimization
        let projected_schema = project_schema(&schema, projections).unwrap();
        
        // Pre-compute plan properties for performance
        let cache = Self::compute_properties(projected_schema.clone());
        
        Self {
            db,
            projected_schema,
            cache,
        }
    }

    /// Computes and caches plan properties for optimization
    /// 
    /// Creates equivalence properties and partitioning information
    /// that DataFusion uses for query optimization and execution planning.
    /// 
    /// # Arguments
    /// * `schema` - Arrow schema to compute properties for
    /// 
    /// # Returns
    /// * `PlanProperties` - Cached properties for this execution plan
    fn compute_properties(schema: SchemaRef) -> PlanProperties {
        // Create equivalence properties for the schema
        let eq_properties = EquivalenceProperties::new(schema);
        
        // Configure plan properties with single partition and bounded execution
        PlanProperties::new(
            eq_properties,
            Partitioning::UnknownPartitioning(1), // Single partition for HTTP data
            ExecutionMode::Bounded,               // Bounded data set (not streaming)
        )
    }
}

// ============================================================================
// Display Implementation
// ============================================================================

impl DisplayAs for HttpExec {
    /// Formats the execution plan for display in query plans
    /// 
    /// # Arguments
    /// * `_t` - Display format type (unused in this simple implementation)
    /// * `f` - Formatter to write the display representation
    /// 
    /// # Returns
    /// * `std::fmt::Result` - Success or formatting error
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> std::fmt::Result {
        write!(f, "HttpExec")
    }
}

// ============================================================================
// DataFusion ExecutionPlan Implementation
// ============================================================================

impl ExecutionPlan for HttpExec {
    /// Returns this instance as Any trait for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Returns the projected schema for this execution plan
    fn schema(&self) -> SchemaRef {
        self.projected_schema.clone()
    }

    /// Creates a new execution plan with different children
    /// 
    /// Since HttpExec is a leaf node (no children), this simply returns self.
    /// 
    /// # Arguments
    /// * `_children` - New child execution plans (unused for leaf nodes)
    /// 
    /// # Returns
    /// * `Result<Arc<dyn ExecutionPlan>>` - New execution plan instance
    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    /// Executes the plan and returns a stream of record batches
    /// 
    /// This is the core execution method that converts JSON data from the
    /// HTTP source into Arrow RecordBatches for query processing.
    /// 
    /// # Arguments
    /// * `_partition` - Partition index to execute (unused for single partition)
    /// * `_context` - Task execution context (unused in this implementation)
    /// 
    /// # Returns
    /// * `Result<SendableRecordBatchStream>` - Stream of Arrow RecordBatches
    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        // Lock the HTTP data source to access the fetched data
        let inner = self.db.inner.lock().unwrap();
        let schema = self.schema();
        let data = &inner.data; // Use a reference to avoid consuming data

        // Convert JSON data to JSON Lines format for Arrow processing
        // Each JSON object becomes a separate line in the string
        let json_string = data
            .iter()
            .map(|value| value.to_string()) // Convert each Value to JSON string
            .collect::<Vec<String>>()
            .join("\n"); // Join with newlines for JSON Lines format

        // Create a cursor reader from the JSON Lines string
        let reader = Cursor::new(json_string);

        // Build Arrow JSON reader with the projected schema
        let mut arrow_reader = ReaderBuilder::new(schema.clone())
            .build(reader)
            .map_err(|e| {
                datafusion::error::DataFusionError::Execution(format!(
                    "Failed to build JSON reader: {}",
                    e
                ))
            })?;

        // Read all record batches from the Arrow JSON reader
        let mut batches = Vec::new();
        while let Some(batch_result) = arrow_reader.next() {
            let batch = batch_result.map_err(|e| {
                datafusion::error::DataFusionError::Execution(format!(
                    "Failed to read RecordBatch: {}",
                    e
                ))
            })?;
            batches.push(batch);
        }

        // Create a memory stream from the collected batches
        let memory_stream = MemoryStream::try_new(batches, schema.clone(), None)
            .map_err(|e| {
                datafusion::error::DataFusionError::Execution(format!(
                    "Failed to create MemoryStream: {}",
                    e
                ))
            })?;

        // Return the memory stream as a sendable record batch stream
        Ok(Box::pin(memory_stream) as SendableRecordBatchStream)
    }

    /// Returns the name of this execution plan for debugging and display
    fn name(&self) -> &str {
        "HttpExec"
    }

    /// Returns the cached plan properties for optimization
    fn properties(&self) -> &datafusion::physical_plan::PlanProperties {
        &self.cache
    }

    /// Returns child execution plans (empty for leaf nodes)
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![] // HttpExec is a leaf node with no children
    }
}