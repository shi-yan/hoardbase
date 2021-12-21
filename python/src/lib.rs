use hoardbase::base::CollectionTrait;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyDateTime, PyDict, PyInt, PyList, PyString, PyTuple};
use serde_json::Map;
use std::cell::RefCell;
use std::sync::{Arc, Mutex};
#[pyclass]
struct Database {
    db: Arc<Mutex<hoardbase::database::Database>>,
}

#[pyclass]
struct Collection {
    name: String,
    db: Arc<Mutex<hoardbase::database::Database>>,
}

fn pydict2serde_json_map(dict: &PyDict, py: pyo3::prelude::Python<'_>) -> serde_json::Map<String, serde_json::Value> {
    let mut map = serde_json::Map::new();

    for (key, value) in dict.iter() {
        let key = key.to_string();
        if value.is_instance::<pyo3::types::PyString>().unwrap() {
            let value_str = value.downcast::<pyo3::types::PyString>().unwrap().to_string();
            map.insert(key, serde_json::Value::String(value_str));
        } else if value.is_instance::<pyo3::types::PyList>().unwrap() || value.is_instance::<pyo3::types::PySet>().unwrap() {
        } else if value.is_instance::<pyo3::types::PyFloat>().unwrap() {
            let val = value.downcast::<pyo3::types::PyFloat>().unwrap();
            map.insert(key, serde_json::Value::Number(serde_json::Number::from_f64(val.value()).unwrap()));
        } else if value.is_instance::<pyo3::types::PyInt>().unwrap() {
            let val = value.downcast::<pyo3::types::PyInt>().unwrap().to_object(py).extract::<i64>(py).unwrap();
            map.insert(key, serde_json::Value::Number(serde_json::Number::from(val)));
        } else if value.is_instance::<pyo3::types::PyBool>().unwrap() {
            let val = value.downcast::<pyo3::types::PyBool>().unwrap();
            map.insert(key, serde_json::Value::Bool(val.is_true()));
        } else if value.is_instance::<pyo3::types::PyDict>().unwrap() {
            let nested = pydict2serde_json_map(&value.downcast::<PyDict>().unwrap(), py);
            map.insert(key, serde_json::Value::Object(nested));
        } else {
            // return Err(0);
        }
    }
    return map;
}
/*
fn pylist2serde_json_vec(list: &PyList) -> Vec<serde_json::Value> {

}

fn pytuple2serde_json_vec(tuple: &PyTuple) -> Vec<serde_json::Value> {

}*/

#[pymethods]
impl Database {
    #[staticmethod]
    fn open(path: &str, config: Option<&PyDict>) -> PyResult<Self> {
        let mut db_config = hoardbase::database::DatabaseConfig::new(path);
        if config.is_some() {
            if let Some(config_dict) = config {
                if let Some(trace) = config_dict.get_item("trace") {
                    let should_trace = trace.downcast::<PyBool>();
                    db_config.trace(should_trace.unwrap().is_true());
                }
                if let Some(profile) = config_dict.get_item("profile") {
                    let should_profile = profile.downcast::<PyBool>();
                    db_config.profile(should_profile.unwrap().is_true());
                }
            }
            for (key, value) in config.unwrap().iter() {
                println!("{} = {}", key, value);
            }
        }

        println!("{}", path);

        let db = hoardbase::database::Database::open(&db_config).unwrap();
        Ok(Database { db: Arc::new(Mutex::new(db)) })
    }

    pub fn create_collection(&mut self, collection_name: &str, config: Option<&PyDict>) -> PyResult<Collection> {
        let mut ccol = hoardbase::base::CollectionConfig::default(collection_name);
        ccol.hash_document(true);
        ccol.log_last_modified(true);

        match self.db.lock().unwrap().create_collection(collection_name, &ccol) {
            Ok(collection) => Ok(Collection { name: collection_name.to_string(), db: self.db.clone() }),
            Err(e) => Err(PyValueError::new_err(format!("{}", e))),
        }
    }
}

#[pyclass]
struct Record {
    record: hoardbase::base::Record,
}

#[pymethods]
impl Record {
    #[getter]
    fn get_id(&self) -> PyResult<i64> {
        Ok(self.record.id)
    }

    #[getter]
    fn get_hash(&self) -> PyResult<String> {
        Ok(self.record.hash.clone())
    }

    #[getter]
    fn get_last_modified<'a>(&self, py: pyo3::prelude::Python<'a>) -> PyResult<&'a PyDateTime> {
        Ok(PyDateTime::from_timestamp(py, self.record.last_modified.timestamp() as f64, None).unwrap())
    }

    /*#[getter]
    fn get_data(&self) -> PyResult<PyDict> {
        Ok(self.record.data.clone())
    }*/
}

#[pymethods]
impl Collection {
    pub fn insert_one(&self, py: pyo3::prelude::Python<'_>, document: &PyDict) -> PyResult<Record> {
        let val = pydict2serde_json_map(document, py);

        let r = self.db.lock().unwrap().collection(&self.name).unwrap().insert_one(&serde_json::Value::Object(val)).unwrap();

        Ok(Record { record: r.unwrap() })
    }
}

/// A Python module implemented in Rust. The name of this function must match
/// the `lib.name` setting in the `Cargo.toml`, else Python will not be able to
/// import the module.
#[pymodule]
fn hoardbase(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<Database>()?;
    m.add_class::<Collection>()?;
    m.add_class::<Record>()?;

    Ok(())
}
