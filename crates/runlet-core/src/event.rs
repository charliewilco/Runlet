use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum EventKind {
	#[serde(rename = "job.queued")]
	JobQueued,
	#[serde(rename = "job.started")]
	JobStarted,
	#[serde(rename = "stdout")]
	Stdout,
	#[serde(rename = "stderr")]
	Stderr,
	#[serde(rename = "job.completed")]
	JobCompleted,
	#[serde(rename = "job.failed")]
	JobFailed,
	#[serde(rename = "job.canceled")]
	JobCanceled,
	#[serde(rename = "job.cancel_requested")]
	JobCancelRequested,
}

impl EventKind {
	pub fn as_str(&self) -> &'static str {
		match self {
			EventKind::JobQueued => "job.queued",
			EventKind::JobStarted => "job.started",
			EventKind::Stdout => "stdout",
			EventKind::Stderr => "stderr",
			EventKind::JobCompleted => "job.completed",
			EventKind::JobFailed => "job.failed",
			EventKind::JobCanceled => "job.canceled",
			EventKind::JobCancelRequested => "job.cancel_requested",
		}
	}
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventRecord {
	pub seq: u64,
	pub ts: DateTime<Utc>,
	pub kind: EventKind,
	pub payload: Value,
}

#[derive(Debug, Clone)]
pub struct EventInput {
	pub ts: DateTime<Utc>,
	pub kind: EventKind,
	pub payload: Value,
}

impl EventInput {
	pub fn new(kind: EventKind, payload: Value) -> Self {
		Self {
			ts: Utc::now(),
			kind,
			payload,
		}
	}
}
