//! PyO3 extension module exposing Rust-accelerated MongoDB internals to Python.
#![forbid(unsafe_code)]

mod aggregation;
mod aggregation_joins;
mod bson_helpers;
mod cached_modules;
mod engine_errors;
mod geo_polygon;
mod geo_query;
mod geo_s2;
mod index_encoding;
mod index_helpers;
mod locking;
mod objectid;
mod oplog;
mod paths;
mod query_compiler;
mod query_expressions;
mod query_update;
mod raw_bson;
pub(crate) mod rbac;
mod redb_client;
mod results;
pub(crate) mod schema;
pub(crate) mod scram;
mod storage;
mod sync_manager;
mod sync_utils;
mod transaction;
mod wire_codec;
mod wire_commands;
mod wire_context;
mod wire_cursors;
mod wire_dispatch;
mod wire_errors;
mod wire_msg;
mod wire_profiler;
mod wire_server;
mod wire_sessions;
mod wire_transactions;

use pyo3::prelude::*;

fn register_core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<objectid::ObjectId>()?;
    m.add_function(wrap_pyfunction!(paths::get_value, m)?)?;
    m.add_function(wrap_pyfunction!(paths::field_exists, m)?)?;
    m.add_function(wrap_pyfunction!(paths::set_value, m)?)?;
    m.add_function(wrap_pyfunction!(paths::unset_value, m)?)?;
    m.add_function(wrap_pyfunction!(bson_helpers::to_bson, m)?)?;
    m.add_function(wrap_pyfunction!(bson_helpers::from_bson, m)?)?;
    m.add_class::<query_compiler::CompiledQuery>()?;
    m.add_function(wrap_pyfunction!(query_compiler::compile_query, m)?)?;
    m.add_function(wrap_pyfunction!(query_expressions::resolve_expr, m)?)?;
    m.add_function(wrap_pyfunction!(query_update::apply_update, m)?)?;
    m.add_function(wrap_pyfunction!(index_encoding::sortable_encode, m)?)?;
    m.add_function(wrap_pyfunction!(index_encoding::invert_encoded, m)?)?;
    m.add_function(wrap_pyfunction!(index_encoding::encode_index_key, m)?)?;
    m.add_function(wrap_pyfunction!(
        index_encoding::encode_index_key_prefix,
        m
    )?)?;
    m.add(
        "ValidationError",
        m.py().get_type::<schema::ValidationError>(),
    )?;
    m.add_function(wrap_pyfunction!(schema::py_validate_document, m)?)?;
    Ok(())
}

fn register_storage(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<locking::ReadWriteLock>()?;
    m.add_class::<results::InsertResult>()?;
    m.add_class::<results::UpdateResult>()?;
    m.add_class::<results::DeleteResult>()?;
    m.add_function(wrap_pyfunction!(storage::scan_and_filter, m)?)?;
    m.add_function(wrap_pyfunction!(storage::scan_and_filter_batch, m)?)?;
    m.add_function(wrap_pyfunction!(storage::batch_insert, m)?)?;
    m.add_function(wrap_pyfunction!(storage::batch_update, m)?)?;
    m.add_function(wrap_pyfunction!(storage::cursor_get_doc, m)?)?;
    m.add_function(wrap_pyfunction!(storage::doc_to_bson, m)?)?;
    m.add_function(wrap_pyfunction!(storage::match_doc, m)?)?;
    m.add_class::<redb_client::RedbLocalClient>()?;
    m.add_class::<redb_client::RedbLocalDB>()?;
    m.add_class::<redb_client::RedbLocalCollection>()?;
    m.add(
        "DuplicateKeyError",
        m.py().get_type::<index_helpers::DuplicateKeyError>(),
    )?;
    m.add_function(wrap_pyfunction!(index_helpers::rs_tokenize, m)?)?;
    m.add_function(wrap_pyfunction!(index_helpers::rs_hash_value, m)?)?;
    m.add_function(wrap_pyfunction!(index_helpers::rs_flatten_doc, m)?)?;
    Ok(())
}

fn register_aggregation(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(aggregation::group_stage, m)?)?;
    m.add_function(wrap_pyfunction!(aggregation::sort_stage, m)?)?;
    m.add_function(wrap_pyfunction!(aggregation::unwind_stage, m)?)?;
    m.add_function(wrap_pyfunction!(aggregation::project_stage, m)?)?;
    m.add_function(wrap_pyfunction!(aggregation::add_fields_stage, m)?)?;
    m.add_function(wrap_pyfunction!(aggregation::match_stage, m)?)?;
    m.add_function(wrap_pyfunction!(aggregation::limit_stage, m)?)?;
    m.add_function(wrap_pyfunction!(aggregation::skip_stage, m)?)?;
    m.add_function(wrap_pyfunction!(aggregation::count_stage, m)?)?;
    m.add_function(wrap_pyfunction!(aggregation::sample_stage, m)?)?;
    m.add_function(wrap_pyfunction!(aggregation::replace_root_stage, m)?)?;
    m.add_function(wrap_pyfunction!(aggregation::unset_stage, m)?)?;
    m.add_function(wrap_pyfunction!(aggregation::redact_stage, m)?)?;
    m.add_function(wrap_pyfunction!(aggregation::sort_by_count_stage, m)?)?;
    m.add_function(wrap_pyfunction!(aggregation::bucket_stage, m)?)?;
    m.add_function(wrap_pyfunction!(aggregation::bucket_auto_stage, m)?)?;
    m.add_function(wrap_pyfunction!(aggregation::set_window_fields_stage, m)?)?;
    m.add_function(wrap_pyfunction!(aggregation::aggregate_pipeline, m)?)?;
    m.add_function(wrap_pyfunction!(aggregation_joins::lookup_stage, m)?)?;
    m.add_function(wrap_pyfunction!(aggregation_joins::graph_lookup_stage, m)?)?;
    m.add_function(wrap_pyfunction!(aggregation_joins::facet_stage, m)?)?;
    m.add_function(wrap_pyfunction!(
        aggregation_joins::pipeline_lookup_stage,
        m
    )?)?;
    Ok(())
}

fn register_sync(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<oplog::OplogHub>()?;
    m.add_class::<oplog::ChangeStream>()?;
    m.add_class::<sync_utils::VectorClock>()?;
    m.add_class::<sync_utils::TombstoneRegistry>()?;
    m.add_function(wrap_pyfunction!(sync_utils::doc_checksum, m)?)?;
    m.add_function(wrap_pyfunction!(sync_utils::diff_fields, m)?)?;
    m.add_function(wrap_pyfunction!(sync_utils::lww, m)?)?;
    m.add_function(wrap_pyfunction!(sync_utils::local_wins, m)?)?;
    m.add_function(wrap_pyfunction!(sync_utils::remote_wins, m)?)?;
    m.add_function(wrap_pyfunction!(sync_utils::field_merge, m)?)?;
    m.add_function(wrap_pyfunction!(sync_utils::crdt_counter_merge, m)?)?;
    m.add_function(wrap_pyfunction!(sync_utils::crdt_set_merge, m)?)?;
    m.add_function(wrap_pyfunction!(sync_utils::crdt_merge_doc, m)?)?;
    m.add_function(wrap_pyfunction!(sync_utils::ejson_default, m)?)?;
    m.add_function(wrap_pyfunction!(sync_utils::ejson_object_hook, m)?)?;
    m.add_function(wrap_pyfunction!(sync_manager::to_pymongo, m)?)?;
    m.add_function(wrap_pyfunction!(sync_manager::from_pymongo, m)?)?;
    m.add_function(wrap_pyfunction!(sync_manager::sync_diff, m)?)?;
    Ok(())
}

fn register_wire(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Wire protocol: msg parsing
    m.add(
        "ProtocolError",
        m.py().get_type::<wire_msg::ProtocolError>(),
    )?;
    m.add(
        "ChecksumMismatch",
        m.py().get_type::<wire_msg::ChecksumMismatch>(),
    )?;
    m.add_class::<wire_msg::MsgHeader>()?;
    m.add_function(wrap_pyfunction!(wire_msg::decode_header, m)?)?;
    m.add_function(wrap_pyfunction!(wire_msg::decode_msg, m)?)?;
    m.add_function(wrap_pyfunction!(wire_msg::encode_msg, m)?)?;
    m.add_function(wrap_pyfunction!(wire_msg::decode_query, m)?)?;
    m.add_function(wrap_pyfunction!(wire_msg::encode_reply, m)?)?;
    m.add_function(wrap_pyfunction!(wire_msg::decode_compressed, m)?)?;
    m.add_function(wrap_pyfunction!(wire_msg::encode_compressed, m)?)?;
    m.add_function(wrap_pyfunction!(wire_msg::available_compressors, m)?)?;
    // Wire protocol: BSON codec
    m.add_function(wrap_pyfunction!(wire_codec::normalize_inbound, m)?)?;
    m.add_function(wrap_pyfunction!(wire_codec::normalize_outbound, m)?)?;
    m.add_function(wrap_pyfunction!(wire_codec::normalize_outbound_docs, m)?)?;
    // Wire protocol: errors
    m.add_function(wrap_pyfunction!(wire_errors::error_response, m)?)?;
    m.add_function(wrap_pyfunction!(wire_errors::make_error, m)?)?;
    // Wire protocol: cursors
    m.add_class::<wire_cursors::CursorRegistry>()?;
    m.add("MAX_BSON_OBJECT_SIZE", wire_cursors::MAX_BSON_OBJECT_SIZE)?;
    m.add("MAX_MESSAGE_SIZE", wire_cursors::MAX_MESSAGE_SIZE)?;
    m.add("MAX_WRITE_BATCH_SIZE", wire_cursors::MAX_WRITE_BATCH_SIZE)?;
    // Wire protocol: sessions
    m.add_class::<wire_sessions::SessionRegistry>()?;
    m.add(
        "TooManySessions",
        m.py().get_type::<wire_sessions::TooManySessions>(),
    )?;
    m.add("MAX_SESSIONS", wire_sessions::MAX_SESSIONS)?;
    // Wire protocol: profiler
    m.add_class::<wire_profiler::OpEntry>()?;
    m.add_class::<wire_profiler::OperationTracker>()?;
    m.add_class::<wire_profiler::TopStats>()?;
    m.add_class::<wire_profiler::Profiler>()?;
    // Wire protocol: transactions
    m.add(
        "TransactionError",
        m.py().get_type::<wire_transactions::TransactionError>(),
    )?;
    m.add_class::<wire_transactions::TransactionState>()?;
    m.add_class::<wire_transactions::SessionTransaction>()?;
    m.add_function(wrap_pyfunction!(
        wire_transactions::commit_active_transaction,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        wire_transactions::abort_active_transaction,
        m
    )?)?;
    // Wire protocol: context helpers
    m.add(
        "NamespaceError",
        m.py().get_type::<wire_context::NamespaceError>(),
    )?;
    m.add_function(wrap_pyfunction!(wire_context::validate_namespace, m)?)?;
    m.add_class::<wire_context::LastWriteResult>()?;
    m.add_class::<wire_context::ParameterStore>()?;
    m.add_class::<wire_context::ConnectionCounter>()?;
    m.add_class::<wire_context::FreeMonitoringState>()?;
    m.add_class::<wire_context::ConnectionContext>()?;
    // Wire protocol: dispatch
    m.add_function(wrap_pyfunction!(wire_dispatch::inc_counter, m)?)?;
    m.add_function(wrap_pyfunction!(wire_dispatch::get_opcounters, m)?)?;
    m.add_function(wrap_pyfunction!(wire_dispatch::next_timestamp, m)?)?;
    m.add_function(wrap_pyfunction!(wire_dispatch::rs_dispatch, m)?)?;
    // Wire protocol: Tokio TCP server
    m.add_class::<wire_server::RustWireServer>()?;
    Ok(())
}

#[pyfunction]
fn __build_version__() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

#[pymodule(gil_used = false)]
fn _smongo_core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    register_core(m)?;
    register_storage(m)?;
    register_aggregation(m)?;
    register_sync(m)?;
    register_wire(m)?;
    m.add_function(wrap_pyfunction!(transaction::get_active_txn_session, m)?)?;
    m.add_function(wrap_pyfunction!(__build_version__, m)?)?;
    Ok(())
}
