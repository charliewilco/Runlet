use crate::sse::{sse_stream, EventStreamParams};
use crate::state::AppState;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::Json;
use chrono::{DateTime, Utc};
use runlet_core::{run_job, EventRecord, JobSpec, JobStatus, StoreError};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
pub struct CreateJobRequest {
	pub cmd: String,
	pub args: Vec<String>,
	pub cwd: Option<String>,
	pub env: Option<std::collections::HashMap<String, String>>,
}

#[derive(Debug, Serialize)]
pub struct CreateJobResponse {
	pub job_id: String,
	pub status: String,
}

#[derive(Debug, Serialize)]
pub struct JobStatusResponse {
	pub job_id: String,
	pub status: JobStatus,
	pub created_at: DateTime<Utc>,
	pub started_at: Option<DateTime<Utc>>,
	pub ended_at: Option<DateTime<Utc>>,
	pub exit_code: Option<i32>,
}

#[derive(Debug, Serialize)]
pub struct EventHistoryResponse {
	pub job_id: String,
	pub events: Vec<EventRecord>,
}

#[derive(Debug, Deserialize)]
pub struct HistoryParams {
	pub after_seq: Option<u64>,
	pub limit: Option<usize>,
}

#[derive(Debug, Serialize)]
pub struct CancelResponse {
	pub job_id: String,
	pub status: String,
}

pub async fn health() -> impl IntoResponse {
	(StatusCode::OK, "ok")
}

pub async fn create_job(
	State(state): State<AppState>,
	Json(payload): Json<CreateJobRequest>,
) -> impl IntoResponse {
	let (job_id, runtime) = state.store.create_job().await;

	let spec = JobSpec {
		cmd: payload.cmd,
		args: payload.args,
		cwd: payload.cwd,
		env: payload.env,
	};

	let max_line_bytes = state.max_line_bytes;
	tokio::spawn(async move {
		run_job(runtime, spec, max_line_bytes).await;
	});

	let response = CreateJobResponse {
		job_id,
		status: "queued".to_string(),
	};
	(StatusCode::OK, Json(response))
}

pub async fn get_job(
	State(state): State<AppState>,
	Path(job_id): Path<String>,
) -> impl IntoResponse {
	match state.store.get_job(&job_id).await {
		Some(info) => {
			let response = JobStatusResponse {
				job_id: info.job_id,
				status: info.status,
				created_at: info.created_at,
				started_at: info.started_at,
				ended_at: info.ended_at,
				exit_code: info.exit_code,
			};
			(StatusCode::OK, Json(response)).into_response()
		}
		None => (StatusCode::NOT_FOUND, "not found").into_response(),
	}
}

pub async fn get_event_history(
	State(state): State<AppState>,
	Path(job_id): Path<String>,
	Query(params): Query<HistoryParams>,
) -> impl IntoResponse {
	let after_seq = params.after_seq.unwrap_or(0);
	let limit = params.limit.unwrap_or(500).min(5000);
	match state
		.store
		.get_event_history(&job_id, after_seq, limit)
		.await
	{
		Some(events) => {
			let response = EventHistoryResponse { job_id, events };
			(StatusCode::OK, Json(response)).into_response()
		}
		None => (StatusCode::NOT_FOUND, "not found").into_response(),
	}
}

pub async fn stream_events(
	State(state): State<AppState>,
	Path(job_id): Path<String>,
	headers: HeaderMap,
	params: Query<EventStreamParams>,
) -> impl IntoResponse {
	sse_stream(state, job_id, headers, params.0).await
}

pub async fn cancel_job(
	State(state): State<AppState>,
	Path(job_id): Path<String>,
) -> impl IntoResponse {
	match state.store.cancel_job(&job_id).await {
		Ok(()) => {
			let response = CancelResponse {
				job_id,
				status: "canceling".to_string(),
			};
			(StatusCode::OK, Json(response)).into_response()
		}
		Err(StoreError::NotFound) => (StatusCode::NOT_FOUND, "not found").into_response(),
		Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, "error").into_response(),
	}
}
