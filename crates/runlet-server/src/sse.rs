use crate::state::AppState;
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::IntoResponse;
use runlet_core::EventRecord;
use serde::Deserialize;
use tokio::sync::broadcast;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::{Stream, StreamExt};

#[derive(Debug, Deserialize)]
pub struct EventStreamParams {
	pub last_event_id: Option<u64>,
}

pub async fn sse_stream(
	state: AppState,
	job_id: String,
	headers_in: HeaderMap,
	params: EventStreamParams,
) -> impl IntoResponse {
	let mut headers = HeaderMap::new();
	headers.insert(header::CONTENT_TYPE, "text/event-stream".parse().unwrap());

	let after_seq = last_event_id_from_headers(&headers_in)
		.or(params.last_event_id)
		.unwrap_or(0);
	let history = state
		.store
		.get_event_history(&job_id, after_seq, 10_000)
		.await;
	let history = match history {
		Some(events) => events,
		None => return (StatusCode::NOT_FOUND, "not found").into_response(),
	};

	let live = match state.store.subscribe(&job_id).await {
		Some(rx) => rx,
		None => return (StatusCode::NOT_FOUND, "not found").into_response(),
	};

	let stream = build_stream(history, live);
	let response =
		Sse::new(stream).keep_alive(KeepAlive::new().interval(std::time::Duration::from_secs(10)));
	(headers, response).into_response()
}

fn build_stream(
	history: Vec<EventRecord>,
	live: broadcast::Receiver<EventRecord>,
) -> impl Stream<Item = Result<Event, std::convert::Infallible>> {
	let history_stream = tokio_stream::iter(history.into_iter().map(to_event));
	let live_stream = BroadcastStream::new(live).filter_map(|result| match result {
		Ok(record) => Some(to_event(record)),
		Err(_) => None,
	});
	history_stream.chain(live_stream)
}

fn to_event(record: EventRecord) -> Result<Event, std::convert::Infallible> {
	let event = Event::default()
		.id(record.seq.to_string())
		.event(record.kind.as_str())
		.json_data(record.payload)
		.unwrap_or_else(|_| {
			Event::default()
				.event("job.failed")
				.data("{\"message\":\"event encoding failed\"}")
		});
	Ok(event)
}

fn last_event_id_from_headers(headers: &HeaderMap) -> Option<u64> {
	headers
		.get("Last-Event-ID")
		.or_else(|| headers.get("last-event-id"))
		.and_then(|value| value.to_str().ok())
		.and_then(|value| value.parse::<u64>().ok())
}
