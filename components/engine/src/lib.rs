mod block;
mod engine;
mod ts;

use crate::ts::TS;
use tszv1::DataPoint;

#[derive(Debug)]
pub struct Raw {
    pub table_name: String,
    pub data_point: DataPoint,
}

impl Raw {
    pub fn to_string(&self) -> String {
        format!(
            "{}:{{{},{}}}",
            self.table_name, self.data_point.time, self.data_point.value
        )
    }
}

pub trait Engine {
    fn create_table(&self, ts_name: String);
    fn append(&self, raw: Raw);
    fn get(&self, ts_name: &String) -> Option<TS>;
}

pub use engine::BTreeEngine;

pub fn create_engine(engine_type: &str) -> Option<Box<dyn Engine>> {
    if engine_type.eq("b-tree") {
        Some(Box::new(engine::BTreeEngine::new()))
    } else {
        None
    }
}
