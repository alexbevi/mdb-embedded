// smongo-engine: Pure Rust MongoDB-compatible embedded database engine

pub mod aggregation;
pub mod collection;
pub mod database;
pub mod explain;
pub mod geo;
pub mod index;
pub mod oplog;
pub mod paths;
pub mod planner;
pub mod query;
pub mod schema;
pub mod storage;
pub mod update;

#[cfg(target_arch = "wasm32")]
pub mod wasm_bindings;

// Re-export main types for convenience
pub use collection::{CollectionView, FindCursor};
pub use database::TransactionSession;
pub use storage::{
    DefaultBackend, DefaultSession, MemBackend, MemCursor, MemSession, StorageBackend,
    StorageCursor, StorageError, StorageResult, StorageSession,
};
#[cfg(not(target_arch = "wasm32"))]
pub use storage::{RedbBackend, RedbCursor, RedbSession};
