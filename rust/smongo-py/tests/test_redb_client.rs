//! Integration tests for RedbLocalClient (redb-backed).

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use tempfile::TempDir;

#[test]
#[ignore = "requires a Python that can import _smongo_core (e.g. maturin develop + PYTHONPATH)"]
fn test_redb_client_basic_crud() -> PyResult<()> {
    Python::try_attach(|py| -> PyResult<()> {
        let tempdir = TempDir::new().unwrap();
        let db_path = tempdir.path().to_str().unwrap();

        // Import the module
        let smongo = PyModule::import(py, "_smongo_core")?;
        let client_cls = smongo.getattr("RedbLocalClient")?;

        // Create client
        let client = client_cls.call1((db_path,))?;
        let db = client.call_method1("get_db", ("testdb",))?;
        let coll = db.call_method1("collection", ("users",))?;

        // Insert
        let doc = PyDict::new(py);
        doc.set_item("name", "Alice")?;
        doc.set_item("age", 30)?;
        let result = coll.call_method1("insert_one", (doc,))?;
        let inserted_id = result.get_item("inserted_id")?;
        assert!(!inserted_id.is_none());

        // Find
        let filter = PyDict::new(py);
        filter.set_item("name", "Alice")?;
        let found: Option<Bound<'_, PyDict>> =
            coll.call_method1("find_one", (filter,))?.extract()?;
        assert!(found.is_some());
        let doc = found.unwrap();
        assert_eq!(doc.get_item("name")?.unwrap().extract::<String>()?, "Alice");
        assert_eq!(doc.get_item("age")?.unwrap().extract::<i32>()?, 30);

        // Count
        let empty_filter = PyDict::new(py);
        let count: u64 = coll
            .call_method1("count_documents", (empty_filter,))?
            .extract()?;
        assert_eq!(count, 1);

        // Update
        let filter = PyDict::new(py);
        filter.set_item("name", "Alice")?;
        let update = PyDict::new(py);
        let set_doc = PyDict::new(py);
        set_doc.set_item("age", 31)?;
        update.set_item("$set", set_doc)?;
        coll.call_method1("update_one", (filter, update))?;

        // Verify update
        let filter = PyDict::new(py);
        filter.set_item("name", "Alice")?;
        let updated: Option<Bound<'_, PyDict>> =
            coll.call_method1("find_one", (filter,))?.extract()?;
        assert_eq!(
            updated
                .unwrap()
                .get_item("age")?
                .unwrap()
                .extract::<i32>()?,
            31
        );

        // Delete
        let filter = PyDict::new(py);
        filter.set_item("name", "Alice")?;
        coll.call_method1("delete_one", (filter,))?;

        // Verify deletion
        let empty_filter = PyDict::new(py);
        let count: u64 = coll
            .call_method1("count_documents", (empty_filter,))?
            .extract()?;
        assert_eq!(count, 0);

        // Close
        client.call_method0("close")?;

        Ok(())
    })
    .expect("Python interpreter not available for tests")?;
    Ok(())
}

#[test]
#[ignore = "requires a Python that can import _smongo_core (e.g. maturin develop + PYTHONPATH)"]
fn test_redb_client_indexes() -> PyResult<()> {
    Python::try_attach(|py| -> PyResult<()> {
        let tempdir = TempDir::new().unwrap();
        let db_path = tempdir.path().to_str().unwrap();

        let smongo = PyModule::import(py, "_smongo_core")?;
        let client_cls = smongo.getattr("RedbLocalClient")?;

        let client = client_cls.call1((db_path,))?;
        let db = client.call_method1("get_db", ("testdb",))?;
        let coll = db.call_method1("collection", ("users",))?;

        // Create index
        let keys = PyDict::new(py);
        keys.set_item("email", 1)?;
        let index_name: String = coll.call_method1("create_index", (keys,))?.extract()?;
        assert!(index_name.contains("email"));

        // List indexes
        let indexes: Bound<'_, PyList> = coll.call_method0("list_indexes")?.extract()?;
        assert!(indexes.len() > 0);

        // Drop index
        coll.call_method1("drop_index", (&index_name,))?;

        client.call_method0("close")?;

        Ok(())
    })
    .expect("Python interpreter not available for tests")?;
    Ok(())
}
