use crate::routes::{cancel_job, create_job, get_event_history, get_job, health, stream_events};
use crate::state::AppState;
use axum::routing::{get, post};
use axum::Router;
use tower_http::trace::TraceLayer;

pub fn build_router(state: AppState) -> Router {
	Router::new()
		.route("/health", get(health))
		.route("/jobs", post(create_job))
		.route("/jobs/:job_id", get(get_job))
		.route("/jobs/:job_id/events", get(stream_events))
		.route("/jobs/:job_id/events/history", get(get_event_history))
		.route("/jobs/:job_id/cancel", post(cancel_job))
		.with_state(state)
		.layer(TraceLayer::new_for_http())
}
