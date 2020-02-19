#[macro_use]
extern crate log;
extern crate log4rs;

mod block;
mod engine;
mod ts;

use crate::ts::TS;
use tszv1::DataPoint;

#[derive(Debug)]
pub struct Raw {
    pub table_name: String,
    pub key: String,
    pub data_point: DataPoint,
}

impl Raw {
    pub fn to_string(&self) -> String {
        format!(
            "{}:{},{{{},{}}}",
            self.table_name, self.key, self.data_point.time, self.data_point.value
        )
    }
}

pub trait Engine {
    fn create_key(&self, raw: Raw);
    fn append(&self, raw: Raw);
    fn get(&self, table_name: &String, key: &String) -> Option<TS>;
}

pub fn create_engine(engine_type: &str) -> Option<Box<dyn Engine + Send + Sync>> {
    if engine_type.eq("b-tree") {
        Some(Box::new(engine::BTreeEngine::new()))
    } else {
        None
    }
}
