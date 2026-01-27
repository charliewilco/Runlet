use runlet_core::Store;

#[derive(Clone)]
pub struct AppState {
	pub store: Store,
	pub max_line_bytes: usize,
}

impl AppState {
	pub fn new(store: Store, max_line_bytes: usize) -> Self {
		Self {
			store,
			max_line_bytes,
		}
	}
}
