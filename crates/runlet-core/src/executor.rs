use crate::event::{EventInput, EventKind};
use crate::store::JobRuntime;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;
use tokio::time::Duration;
use tracing::{error, warn};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobSpec {
	pub cmd: String,
	pub args: Vec<String>,
	pub cwd: Option<String>,
	pub env: Option<HashMap<String, String>>,
}

pub async fn run_job(runtime: JobRuntime, spec: JobSpec, max_line_bytes: usize) {
	if *runtime.cancel_rx.borrow() {
		let _ = runtime
			.event_tx
			.send(EventInput::new(EventKind::JobCanceled, json!({})))
			.await;
		return;
	}

	let mut cmd = Command::new(&spec.cmd);
	cmd.args(&spec.args);
	if let Some(cwd) = &spec.cwd {
		cmd.current_dir(cwd);
	}
	if let Some(env) = &spec.env {
		cmd.envs(env);
	}
	cmd.stdout(std::process::Stdio::piped());
	cmd.stderr(std::process::Stdio::piped());

	let mut child = match cmd.spawn() {
		Ok(child) => child,
		Err(err) => {
			let _ = runtime
				.event_tx
				.send(EventInput::new(
					EventKind::JobFailed,
					json!({"message": err.to_string()}),
				))
				.await;
			return;
		}
	};

	let _ = runtime
		.event_tx
		.send(EventInput::new(EventKind::JobStarted, json!({})))
		.await;

	if let Some(stdout) = child.stdout.take() {
		let tx = runtime.event_tx.clone();
		tokio::spawn(async move {
			let mut lines = BufReader::new(stdout).lines();
			loop {
				match lines.next_line().await {
					Ok(Some(line)) => {
						let line = truncate_line(&line, max_line_bytes);
						let _ = tx
							.send(EventInput::new(EventKind::Stdout, json!({"line": line})))
							.await;
					}
					Ok(None) => break,
					Err(err) => {
						warn!("stdout read error: {err}");
						break;
					}
				}
			}
		});
	}

	if let Some(stderr) = child.stderr.take() {
		let tx = runtime.event_tx.clone();
		tokio::spawn(async move {
			let mut lines = BufReader::new(stderr).lines();
			loop {
				match lines.next_line().await {
					Ok(Some(line)) => {
						let line = truncate_line(&line, max_line_bytes);
						let _ = tx
							.send(EventInput::new(EventKind::Stderr, json!({"line": line})))
							.await;
					}
					Ok(None) => break,
					Err(err) => {
						warn!("stderr read error: {err}");
						break;
					}
				}
			}
		});
	}

	let mut cancel_rx = runtime.cancel_rx.clone();
	let mut canceling = false;

	loop {
		tokio::select! {
			status = child.wait() => {
				match status {
					Ok(status) => {
						if canceling {
							let _ = runtime
								.event_tx
								.send(EventInput::new(EventKind::JobCanceled, json!({})))
								.await;
						} else {
							let exit_code = status.code().unwrap_or(-1);
							let _ = runtime
								.event_tx
								.send(EventInput::new(
									EventKind::JobCompleted,
									json!({"exit_code": exit_code}),
								))
								.await;
						}
					}
					Err(err) => {
						error!("wait error: {err}");
						let _ = runtime
							.event_tx
							.send(EventInput::new(
								EventKind::JobFailed,
								json!({"message": err.to_string()}),
							))
							.await;
					}
				}
				break;
			}
			_ = cancel_rx.changed(), if !canceling => {
				if *cancel_rx.borrow() {
					canceling = true;
					let _ = request_cancel(&mut child).await;
				}
			}
		}
	}
}

async fn request_cancel(child: &mut tokio::process::Child) -> std::io::Result<()> {
	#[cfg(unix)]
	{
		if let Some(id) = child.id() {
			unsafe {
				libc::kill(id as i32, libc::SIGTERM);
			}
		}
		tokio::time::sleep(Duration::from_millis(1500)).await;
		if child.try_wait()?.is_none() {
			let _ = child.kill().await;
		}
	}

	#[cfg(windows)]
	{
		let _ = child.kill().await;
	}

	#[cfg(not(any(unix, windows)))]
	{
		let _ = child.kill().await;
	}

	Ok(())
}

fn truncate_line(line: &str, max_line_bytes: usize) -> String {
	let bytes = line.as_bytes();
	if bytes.len() <= max_line_bytes {
		return line.to_string();
	}
	String::from_utf8_lossy(&bytes[..max_line_bytes]).to_string()
}
