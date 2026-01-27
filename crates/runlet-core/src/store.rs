use crate::event::{EventInput, EventKind, EventRecord};
use crate::job::{JobId, JobInfo, JobStatus};
use chrono::Utc;
use serde_json::json;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc, watch, RwLock};

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("job not found")]
    NotFound,
    #[error("event channel closed")]
    EventChannelClosed,
}

#[derive(Debug, Clone)]
pub struct Store {
    inner: Arc<RwLock<HashMap<JobId, JobEntry>>>,
    max_events_per_job: usize,
}

#[derive(Debug)]
struct JobEntry {
    info: JobInfo,
    events: VecDeque<EventRecord>,
    next_seq: u64,
    event_tx: mpsc::Sender<EventInput>,
    broadcast_tx: broadcast::Sender<EventRecord>,
    cancel_tx: watch::Sender<bool>,
}

#[derive(Debug, Clone)]
pub struct JobRuntime {
    pub job_id: JobId,
    pub event_tx: mpsc::Sender<EventInput>,
    pub cancel_rx: watch::Receiver<bool>,
}

impl Store {
    pub fn new(max_events_per_job: usize) -> Self {
        Self {
            inner: Arc::new(RwLock::new(HashMap::new())),
            max_events_per_job,
        }
    }

    pub async fn create_job(&self) -> (JobId, JobRuntime) {
        let job_id = ulid::Ulid::new().to_string();
        let (event_tx, event_rx) = mpsc::channel(256);
        let (broadcast_tx, _) = broadcast::channel(1024);
        let (cancel_tx, cancel_rx) = watch::channel(false);

        let now = Utc::now();
        let info = JobInfo {
            job_id: job_id.clone(),
            status: JobStatus::Queued,
            created_at: now,
            started_at: None,
            ended_at: None,
            exit_code: None,
        };

        let entry = JobEntry {
            info,
            events: VecDeque::new(),
            next_seq: 0,
            event_tx: event_tx.clone(),
            broadcast_tx: broadcast_tx.clone(),
            cancel_tx,
        };

        self.inner.write().await.insert(job_id.clone(), entry);

        let store = self.clone();
        let job_id_for_sequencer = job_id.clone();
        tokio::spawn(async move {
            store.run_sequencer(job_id_for_sequencer, event_rx).await;
        });

        let runtime = JobRuntime {
            job_id: job_id.clone(),
            event_tx,
            cancel_rx,
        };

        let _ = runtime
            .event_tx
            .send(EventInput::new(EventKind::JobQueued, json!({})))
            .await;

        (job_id, runtime)
    }

    async fn run_sequencer(&self, job_id: JobId, mut event_rx: mpsc::Receiver<EventInput>) {
        while let Some(input) = event_rx.recv().await {
            let mut guard = self.inner.write().await;
            let entry = match guard.get_mut(&job_id) {
                Some(entry) => entry,
                None => return,
            };

            entry.next_seq += 1;
            let record = EventRecord {
                seq: entry.next_seq,
                ts: input.ts,
                kind: input.kind,
                payload: input.payload,
            };

            match record.kind {
                EventKind::JobQueued => {
                    entry.info.status = JobStatus::Queued;
                }
                EventKind::JobStarted => {
                    entry.info.status = JobStatus::Running;
                    entry.info.started_at = Some(record.ts);
                }
                EventKind::JobCompleted => {
                    entry.info.status = JobStatus::Completed;
                    entry.info.ended_at = Some(record.ts);
                    entry.info.exit_code = record
                        .payload
                        .get("exit_code")
                        .and_then(|v| v.as_i64())
                        .map(|v| v as i32);
                }
                EventKind::JobFailed => {
                    entry.info.status = JobStatus::Failed;
                    entry.info.ended_at = Some(record.ts);
                }
                EventKind::JobCanceled => {
                    entry.info.status = JobStatus::Canceled;
                    entry.info.ended_at = Some(record.ts);
                }
                EventKind::Stdout | EventKind::Stderr | EventKind::JobCancelRequested => {}
            }

            if entry.events.len() >= self.max_events_per_job {
                entry.events.pop_front();
            }
            entry.events.push_back(record.clone());
            let _ = entry.broadcast_tx.send(record);
        }
    }

    pub async fn get_job(&self, job_id: &str) -> Option<JobInfo> {
        self.inner
            .read()
            .await
            .get(job_id)
            .map(|entry| entry.info.clone())
    }

    pub async fn get_event_history(
        &self,
        job_id: &str,
        after_seq: u64,
        limit: usize,
    ) -> Option<Vec<EventRecord>> {
        let guard = self.inner.read().await;
        let entry = guard.get(job_id)?;
        let mut events = Vec::new();
        for record in entry.events.iter() {
            if record.seq > after_seq {
                events.push(record.clone());
                if events.len() >= limit {
                    break;
                }
            }
        }
        Some(events)
    }

    pub async fn subscribe(&self, job_id: &str) -> Option<broadcast::Receiver<EventRecord>> {
        self.inner
            .read()
            .await
            .get(job_id)
            .map(|entry| entry.broadcast_tx.subscribe())
    }

    pub async fn emit(
        &self,
        job_id: &str,
        kind: EventKind,
        payload: serde_json::Value,
    ) -> Result<(), StoreError> {
        let tx = {
            let guard = self.inner.read().await;
            let entry = guard.get(job_id).ok_or(StoreError::NotFound)?;
            entry.event_tx.clone()
        };
        tx.send(EventInput::new(kind, payload))
            .await
            .map_err(|_| StoreError::EventChannelClosed)
    }

    pub async fn cancel_job(&self, job_id: &str) -> Result<(), StoreError> {
        let cancel_tx = {
            let guard = self.inner.read().await;
            let entry = guard.get(job_id).ok_or(StoreError::NotFound)?;
            entry.cancel_tx.clone()
        };

        let _ = cancel_tx.send(true);
        let _ = self
            .emit(job_id, EventKind::JobCancelRequested, json!({}))
            .await;

        Ok(())
    }
}
