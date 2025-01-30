use std::{any::Any, fmt, io::Cursor, sync::Arc};

use crate::datasources::HttpDataSource;
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

#[derive(Debug, Clone)]
pub struct HttpExec {
    db: HttpDataSource,
    projected_schema: SchemaRef,
    cache: PlanProperties,
}

impl HttpExec {
    pub fn new(
        projections: Option<&Vec<usize>>,
        schema: SchemaRef,
        db: HttpDataSource,
    ) -> Self {
        let projected_schema = project_schema(&schema, projections).unwrap();
        let cache = Self::compute_properties(projected_schema.clone());
        Self {
            db,
            projected_schema,
            cache,
        }
    }

    fn compute_properties(schema: SchemaRef) -> PlanProperties {
        let eq_properties = EquivalenceProperties::new(schema);
        PlanProperties::new(
            eq_properties,
            Partitioning::UnknownPartitioning(1),
            ExecutionMode::Unbounded,
        )
    }
}

impl DisplayAs for HttpExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> std::fmt::Result {
        write!(f, "HttpExec")
    }
}


impl ExecutionPlan for HttpExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.projected_schema.clone()
    }

    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let inner = self.db.inner.lock().unwrap();
        let schema = self.schema();
        let data = &inner.data; // Use a reference to avoid consuming `data`.

        // Serialize the data to a JSON string in JSON Lines format
        let json_string = data
            .iter()
            .map(|value| value.to_string()) // Convert each Value to a JSON string
            .collect::<Vec<String>>()
            .join("\n");

        // Create a reader from the serialized JSON string
        let reader = Cursor::new(json_string);

        // Build the Arrow JSON reader
        let mut arrow_reader = ReaderBuilder::new(schema.clone())
            .build(reader)
            .map_err(|e| {
                datafusion::error::DataFusionError::Execution(format!(
                    "Failed to build JSON reader: {}",
                    e
                ))
            })?;

        // Consume all batches from the Arrow JSON reader
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

        // Wrap the collected batches into a MemoryStream
        let memory_stream = MemoryStream::try_new(batches, schema.clone(), None).map_err(|e| {
            datafusion::error::DataFusionError::Execution(format!(
                "Failed to create MemoryStream: {}",
                e
            ))
        })?;

        // Return the MemoryStream as a SendableRecordBatchStream
        Ok(Box::pin(memory_stream) as SendableRecordBatchStream)
    }

    fn name(&self) -> &str {
        "HttpExec"
    }

    fn properties(&self) -> &datafusion::physical_plan::PlanProperties {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }
}
