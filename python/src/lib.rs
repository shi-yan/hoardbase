use hoardbase::base::CollectionTrait;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyDateTime, PyDict, PyInt, PyList, PyString, PyTuple, PyFunction};
use std::cell::RefCell;
use std::sync::{Arc, Mutex};
use bson::*;
use hoardbase::process_record;
use serde_json::Value;

#[pyclass]
struct Database {
    db: Arc<Mutex<hoardbase::database::Database>>,
}

#[pyclass]
struct Collection {
    name: String,
    db: Arc<Mutex<hoardbase::database::Database>>,
}

fn pydict2bson_document(dict: &PyDict, py: pyo3::prelude::Python<'_>) -> bson::Document {
    let mut map = bson::Document::new();

    for (key, value) in dict.iter() {
        let key = key.to_string();
        if value.is_instance::<pyo3::types::PyString>().unwrap() {
            let value_str = value.downcast::<pyo3::types::PyString>().unwrap().to_string();
            map.insert(key, bson::Bson::String(value_str));
        } else if value.is_instance::<pyo3::types::PyList>().unwrap() || value.is_instance::<pyo3::types::PySet>().unwrap() {
        } else if value.is_instance::<pyo3::types::PyFloat>().unwrap() {
            let val = value.downcast::<pyo3::types::PyFloat>().unwrap();
            map.insert(key, bson::Bson::Double(val.value()));
        } else if value.is_instance::<pyo3::types::PyInt>().unwrap() {
            let val = value.downcast::<pyo3::types::PyInt>().unwrap().to_object(py).extract::<i64>(py).unwrap();
            map.insert(key, bson::Bson::Int64(val));
        } else if value.is_instance::<pyo3::types::PyBool>().unwrap() {
            let val = value.downcast::<pyo3::types::PyBool>().unwrap();
            map.insert(key, bson::Bson::Boolean(val.is_true()));
        } else if value.is_instance::<pyo3::types::PyDict>().unwrap() {
            let nested = pydict2bson_document(&value.downcast::<PyDict>().unwrap(), py);
            map.insert(key, bson::Bson::Document(nested));
        } else {
            // return Err(0);
        }
    }
    return map;
}


fn bson_array2pylist<'a>(array: &bson::Array, py: pyo3::prelude::Python<'a>) -> &'a PyList {
    let mut list = PyList::empty(py);
    
    for v in array {
        match v {
            bson::Bson::String(s) => {
                list.append(s);
            }
            bson::Bson::Double(d) => {
                list.append(d);
            }
            bson::Bson::Int64(i) => {
                list.append(i);
            }
            bson::Bson::Boolean(b) => {
                list.append(b);
            }
            bson::Bson::Document(doc) => {
                let mut py_dict_nested = bson_document2pydict(doc, py);
                list.append(py_dict_nested);
            }
            bson::Bson::Array(arr) => {
                let mut py_list_nested = bson_array2pylist(arr, py);
                list.append(py_list_nested);
            }
            _ => {
                // return Err(0);
            }
        }
    }

    return list;
}


fn bson_document2pydict<'a>(dict: &bson::Document, py: pyo3::prelude::Python<'a>) -> &'a PyDict {
    let mut py_dict = PyDict::new(py);
    for (key, value) in dict {
        if bson::Bson::Null == *value {
            py_dict.set_item(key, py.None()).unwrap();
        }
        else if let bson::Bson::Int32(val) = value {
            py_dict.set_item(key, val).unwrap();
        }
        else if let bson::Bson::Int64(val) = value {
            py_dict.set_item(key, val).unwrap();
        }
        else if let bson::Bson::Double(val) = value {
            py_dict.set_item(key, val).unwrap();
        }
        else if let bson::Bson::String(val_str) = value {
            py_dict.set_item(key, val_str).unwrap();
        }
        else if let bson::Bson::Document(doc) = value {
            let mut py_dict_nested = bson_document2pydict(doc, py);
            py_dict.set_item(key, py_dict_nested).unwrap();
        }
        else if let bson::Bson::Array(arr) = value {
            let py_list = bson_array2pylist(arr, py);
            py_dict.set_item(key, py_list).unwrap();
        }
    }
    return py_dict;
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

    pub fn collection(&mut self, collection_name: &str) -> PyResult<Collection> {
        println!("python find {}", collection_name);
        match self.db.lock().unwrap().collection(collection_name) {
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

    #[getter]
    fn get_data<'a>(&self, py: pyo3::prelude::Python<'a>) -> PyResult<&'a PyDict> {
        let r = bson_document2pydict(&self.record.data, py);
        Ok(r)
    }
}

#[pyclass]
struct Index {
    index: hoardbase::base::Index,
}

#[pymethods]
impl Index {
    #[getter]
    fn get_seq(&self) -> PyResult<i64> {
        Ok(self.index.seq)
    }

    #[getter]
    fn get_name(&self) -> PyResult<String> {
        Ok(self.index.name.clone())
    }

    #[getter]
    fn get_is_unique(&self) -> PyResult<bool> {
        Ok(self.index.is_unique)
    }

    #[getter]
    fn get_index_type(&self) -> PyResult<String> {
        Ok(self.index.index_type.clone())
    }

    #[getter]
    fn get_is_partial(&self) -> PyResult<bool> {
        Ok(self.index.is_partial)
    }
}

#[pymethods]
impl Collection {
    pub fn insert_one(&self, py: pyo3::prelude::Python<'_>, document: &PyDict) -> PyResult<Record> {
        let val = pydict2bson_document(document, py);
        let r = self.db.lock().unwrap().collection(&self.name).unwrap().insert_one(&val).unwrap();
        Ok(Record { record: r.unwrap() })
    }

    pub fn find(&mut self,  py: pyo3::prelude::Python<'_>, query: &PyDict, f: &PyFunction, options: Option<&PyDict>) -> PyResult<()> {
        let mut query_bson = pydict2bson_document(query, py);
        self.db.lock().unwrap().collection(&self.name).unwrap().find(&query_bson, &None, process_record!( record => {
            let v = Record {record : record.clone()};
            let args = (v,);
            f.call1( args ).unwrap();  
            Ok(())
        }) ).unwrap();
        Ok(())
    }

    fn count_documents(&mut self, py: pyo3::prelude::Python<'_>, query: &PyDict, options: Option<&PyDict>) -> PyResult<i64> {
        let c = self.db.lock().unwrap().collection(&self.name).unwrap().count_documents(&pydict2bson_document(query, py), &None).unwrap();
        Ok(c)
    }

    fn create_index(&mut self, py: pyo3::prelude::Python<'_>, config: &PyDict, is_unique: bool) -> PyResult<()> {
        self.db.lock().unwrap().collection(&self.name).unwrap().create_index(&pydict2bson_document(config, py), is_unique).unwrap();
        Ok(())
    }

    fn get_name(&mut self, py: pyo3::prelude::Python<'_>) -> PyResult<String> {
        Ok(self.db.lock().unwrap().collection(&self.name).unwrap().get_name().to_string())
    }

    fn get_table_name(&mut self, py: pyo3::prelude::Python<'_>) -> PyResult<String> {
        Ok(self.db.lock().unwrap().collection(&self.name).unwrap().get_table_name().to_string())
    }

    fn delete_one(&mut self, py: pyo3::prelude::Python<'_>, query: &PyDict) -> PyResult<usize> {
        let c = self.db.lock().unwrap().collection(&self.name).unwrap().delete_one(&pydict2bson_document(query, py)).unwrap();
        Ok(c)
    }

    fn changes(&mut self, py: pyo3::prelude::Python<'_>) -> PyResult<i64> {
        let c = self.db.lock().unwrap().collection(&self.name).unwrap().changes().unwrap();
        Ok(c)
    }

    fn delete_many(&mut self,py: pyo3::prelude::Python<'_>, query: &PyDict) -> PyResult<usize> {
        let c = self.db.lock().unwrap().collection(&self.name).unwrap().delete_many(&pydict2bson_document(query, py)).unwrap();
        Ok(c)
    }

    fn distinct(&mut self,py: pyo3::prelude::Python<'_>, field: &str, query: Option<&PyDict>, options: Option<&PyDict>) -> PyResult<i64> {
        let c = self.db.lock().unwrap().collection(&self.name).unwrap().distinct(&field, &Some(pydict2bson_document(query.unwrap(), py)), &None).unwrap();
        Ok(c)
    }

    fn drop_index(&mut self,py: pyo3::prelude::Python<'_>,  index_name: &str) -> PyResult<()> {
        self.db.lock().unwrap().collection(&self.name).unwrap().drop_index(&index_name).unwrap();
        Ok(())
    }

    fn find_one(&mut self,py: pyo3::prelude::Python<'_>, query: &PyDict, skip: i64) -> PyResult<Record> {
        let r = self.db.lock().unwrap().collection(&self.name).unwrap().find_one(&pydict2bson_document(query, py), skip).unwrap();
        Ok(Record { record: r})
    }

    fn find_one_and_delete(&mut self,py: pyo3::prelude::Python<'_>, query: &PyDict) -> PyResult<Option<Record>> {
        if let Ok(r) = self.db.lock().unwrap().collection(&self.name).unwrap().find_one_and_delete(&pydict2bson_document(query, py)) {
            if let Some(rr) = r {
                Ok(Some(Record { record: rr}))
            }
            else {
                Ok(None)
            }
            
        } else {
            Ok(None)
        }
    }

    fn get_indexes(&mut self,py: pyo3::prelude::Python<'_>) -> PyResult<Vec<Index>> {
        let indexes = self.db.lock().unwrap().collection(&self.name).unwrap().get_indexes().unwrap();
        let mut r = Vec::new();        
        for index in indexes {
            r.push(Index { index: index.clone() });
        }
        Ok(r)
    }

    fn insert_many(&mut self,py: pyo3::prelude::Python<'_>, documents: &PyList) -> PyResult<()> {
        let mut documents_bson = Vec::new();
        for document in documents {
            let document = document.downcast::<PyDict>().unwrap();
            documents_bson.push(pydict2bson_document(document, py));
        }
        self.db.lock().unwrap().collection(&self.name).unwrap().insert_many(&documents_bson).unwrap();
        Ok(())
    }

    fn reindex(&mut self) -> PyResult<()> {
        self.db.lock().unwrap().collection(&self.name).unwrap().reindex().unwrap();
        Ok(())
    }

    fn replace_one(&mut self,py: pyo3::prelude::Python<'_>, query: &PyDict, replacement: &PyDict, skip: i64) -> PyResult<Option<Record>> {
        let r = self.db.lock().unwrap().collection(&self.name).unwrap().replace_one(&pydict2bson_document(query, py), &pydict2bson_document(replacement, py), skip).unwrap();
        if let Some(rr) = r {
            Ok(Some(Record { record: rr}))
        }
        else {
            Ok(None)
        }
    }

    fn update_one(&mut self,py: pyo3::prelude::Python<'_>, query: &PyDict, update: &PyDict, skip: i64, upsert: bool) -> PyResult<Option<Record>> {
        let r = self.db.lock().unwrap().collection(&self.name).unwrap().update_one(&pydict2bson_document(query, py), &pydict2bson_document(update, py), skip, upsert).unwrap();
        if let Some(rr) = r {
            Ok(Some(Record { record: rr}))
        }
        else {
            Ok(None)
        }
    }

    fn update_many(&mut self,py: pyo3::prelude::Python<'_>,query: &PyDict, update: &PyDict, limit: i64, skip: i64, upsert: bool) -> PyResult<i64> {
        let c = self.db.lock().unwrap().collection(&self.name).unwrap().update_many(&pydict2bson_document(query, py), &pydict2bson_document(update, py), limit, skip, upsert).unwrap();
        Ok(c)
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
