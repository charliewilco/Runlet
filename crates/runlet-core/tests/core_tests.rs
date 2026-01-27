use runlet_core::{run_job, EventKind, JobSpec, Store};
use std::collections::HashMap;
use tokio::sync::broadcast;
use tokio::time::{sleep, timeout, Duration};

#[tokio::test]
async fn event_sequencing_is_monotonic() {
	let store = Store::new(1000);
	let (job_id, _runtime) = store.create_job().await;

	store
		.emit(&job_id, EventKind::JobStarted, serde_json::json!({}))
		.await
		.unwrap();
	store
		.emit(
			&job_id,
			EventKind::Stdout,
			serde_json::json!({"line":"hello"}),
		)
		.await
		.unwrap();
	store
		.emit(
			&job_id,
			EventKind::JobCompleted,
			serde_json::json!({"exit_code":0}),
		)
		.await
		.unwrap();

	let mut events = Vec::new();
	for _ in 0..20 {
		if let Some(history) = store.get_event_history(&job_id, 0, 100).await {
			if history.len() >= 4 {
				events = history;
				break;
			}
		}
		sleep(Duration::from_millis(10)).await;
	}

	assert!(events.len() >= 4);
	for window in events.windows(2) {
		assert!(window[0].seq < window[1].seq);
	}
	assert_eq!(events[0].kind, EventKind::JobQueued);
}

#[tokio::test]
async fn run_job_emits_events_and_completes() {
	let store = Store::new(1000);
	let (job_id, runtime) = store.create_job().await;
	let mut rx = store.subscribe(&job_id).await.unwrap();

	let spec = JobSpec {
		cmd: "bash".to_string(),
		args: vec![
			"-lc".to_string(),
			"echo hello && sleep 0.2 && echo world".to_string(),
		],
		cwd: None,
		env: None,
	};

	tokio::spawn(async move {
		run_job(runtime, spec, 16_384).await;
	});

	let events = collect_until_terminal(&mut rx, Duration::from_secs(5)).await;
	let kinds: Vec<_> = events.iter().map(|e| e.kind.clone()).collect();

	assert!(kinds.contains(&EventKind::JobStarted));
	assert!(kinds.contains(&EventKind::Stdout));
	assert!(kinds.contains(&EventKind::JobCompleted));
}

#[tokio::test]
async fn cancel_job_emits_canceled() {
	let store = Store::new(1000);
	let (job_id, runtime) = store.create_job().await;
	let mut rx = store.subscribe(&job_id).await.unwrap();

	let spec = JobSpec {
		cmd: "bash".to_string(),
		args: vec!["-lc".to_string(), "sleep 10".to_string()],
		cwd: None,
		env: Some(HashMap::new()),
	};

	tokio::spawn(async move {
		run_job(runtime, spec, 16_384).await;
	});

	sleep(Duration::from_millis(200)).await;
	store.cancel_job(&job_id).await.unwrap();

	let events = collect_until_terminal(&mut rx, Duration::from_secs(5)).await;
	let mut saw_cancel_requested = false;
	let mut terminal = None;
	for event in events {
		if event.kind == EventKind::JobCancelRequested {
			saw_cancel_requested = true;
		}
		if matches!(
			event.kind,
			EventKind::JobCompleted | EventKind::JobFailed | EventKind::JobCanceled
		) {
			terminal = Some(event.kind);
			break;
		}
	}

	assert!(saw_cancel_requested);
	assert_eq!(terminal, Some(EventKind::JobCanceled));
}

async fn collect_until_terminal(
	rx: &mut broadcast::Receiver<runlet_core::EventRecord>,
	timeout_duration: Duration,
) -> Vec<runlet_core::EventRecord> {
	let mut events = Vec::new();
	let _ = timeout(timeout_duration, async {
		loop {
			match rx.recv().await {
				Ok(event) => {
					events.push(event.clone());
					if matches!(
						event.kind,
						EventKind::JobCompleted | EventKind::JobFailed | EventKind::JobCanceled
					) {
						break;
					}
				}
				Err(broadcast::error::RecvError::Lagged(_)) => continue,
				Err(_) => break,
			}
		}
	})
	.await;
	events
}
