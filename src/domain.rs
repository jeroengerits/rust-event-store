use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct EventInput {
    pub id: Uuid,
    #[serde(rename = "type")]
    pub event_type: String,
    pub data: Value,
    pub metadata: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Event {
    pub id: Uuid,
    pub stream: String,
    pub revision: i64,
    pub position: i64,
    #[serde(rename = "type")]
    pub event_type: String,
    pub data: Value,
    pub metadata: Option<Value>,
    pub timestamp: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AppendRequest {
    pub expected_revision: Option<i64>,
    pub events: Vec<EventInput>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AppendResult {
    pub stream: String,
    pub revision: i64,
    pub first_position: i64,
    pub last_position: i64,
}
