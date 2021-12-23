use std::ptr::null_mut;
use libc::c_char;
use std::ffi::CString;
use std::ffi::CStr;
use std::os::raw::{c_int, c_void};
use serde_json::Value;
use serde_json::Map;
use std::borrow::Borrow;
use serde_json::json;
use hoardbase::base::CollectionTrait;
//https://sixtyfps.io/blog/expose-rust-library-to-other-languages.html

#[repr(C)]
pub enum JsonFieldType {
    String,
    Int,
    Float,
    Object,
    Array,
    Boolean,
    Null,
}

#[no_mangle]
pub unsafe extern "C" fn sixtyfps_shared_vector_free(ptr: *mut u8, size: usize, align: usize) {
    /*...*/
}

#[no_mangle]
pub unsafe extern "C" fn sixtyfps_shared_vector_allocate(size: usize, align: usize) -> *mut u8 {
    /*...*/
    return null_mut();
}

#[no_mangle]
pub unsafe extern "C" fn open(path: *const c_char) -> *mut c_void {
    let c_str: &CStr = CStr::from_ptr(path);
    let str_slice: &str = c_str.to_str().unwrap();
    let str_buf: String = str_slice.to_owned(); 
    println!("{}", str_buf);

    let mut config = hoardbase::database::DatabaseConfig::new(str_slice);
    config.trace(true);
    config.profile(true);
    let mut db = hoardbase::database::Database::open(&config).unwrap();
    let handle = Box::new(db);
    let handle_ptr = Box::<hoardbase::database::Database>::into_raw(handle);            
    println!("created handle");

    return handle_ptr as *mut c_void;
}

#[no_mangle]
pub unsafe extern "C" fn close(handle: *mut c_void) {
    let handle = Box::from_raw(handle as *mut hoardbase::database::Database);
    println!("closed handle");
}

#[no_mangle]
pub unsafe extern "C" fn create_collection(handle: *mut c_void, name: *const c_char) -> *mut c_void {
    let handle = handle as *mut hoardbase::database::Database;
    println!("created collection1");
    let c_str: &CStr = CStr::from_ptr(name);
    let str_slice: &str = c_str.to_str().unwrap();
    println!("created collection2 {}", str_slice);
    let collection = (*handle).create_collection(str_slice, &hoardbase::base::CollectionConfig::default(str_slice)).unwrap();
    println!("created collection");
    let collection_ptr = Box::new(collection);
    let collection_ptr = Box::<hoardbase::collection::Collection>::into_raw(collection_ptr);
    return collection_ptr as *mut c_void;
}

#[no_mangle]
pub unsafe extern "C" fn collection_insert_one(handle: *mut c_void, val: *const c_void) -> *mut c_void {
    let handle = handle as *mut hoardbase::collection::Collection;
    let val = val as *mut serde_json::Value;
    if let Ok(inserted) = (*handle).insert_one(&*val) {
        
        let inserted_ptr = Box::new(inserted.unwrap());
        let inserted_ptr = Box::<hoardbase::base::Record>::into_raw(inserted_ptr);
        return inserted_ptr as *mut c_void;
    }
    else {
        return null_mut();
    }
    
}



#[no_mangle]
pub unsafe extern "C" fn serde_json_map_new() -> *mut c_void {
    let handle = Box::new(serde_json::Map::<String, serde_json::Value>::new());
    let handle_ptr = Box::<serde_json::Map<String, serde_json::Value>>::into_raw(handle);

    return handle_ptr as *mut c_void;
}

#[no_mangle]
pub unsafe extern "C" fn serde_json_map_drop(ptr: *mut c_void) {
    let handle_ptr = Box::<serde_json::Map<String, serde_json::Value>>::from_raw(ptr as *mut serde_json::Map<String, serde_json::Value>);
}

#[no_mangle]
pub unsafe extern "C" fn serde_json_map_field_count(ptr: *mut c_void) -> usize {
    let handle_ptr = Box::<serde_json::Map<String, serde_json::Value>>::from_raw(ptr as *mut serde_json::Map<String, serde_json::Value>);

    let count = handle_ptr.len() as usize;

    let handle_ptr = Box::<serde_json::Map<String, serde_json::Value>>::into_raw(handle_ptr);

    return count;
}
/*
#[no_mangle]
pub unsafe extern "C" fn serde_json_map_key(ptr: *mut c_void, index: usize) -> CString {
    let handle_ptr = Box::<serde_json::Map<String, serde_json::Value>>::from_raw(ptr as *mut serde_json::Map<String, serde_json::Value>);

    let mut keys = handle_ptr.keys();

    let c_str = keys.nth(index).unwrap().as_str();

    let c_string = CString::new(&c_str[..]).unwrap() ;
    
    return c_string;
}*/

#[no_mangle]
pub unsafe extern "C" fn serde_json_map_iter_next(ptr: *mut c_void) -> *mut c_void {
    let handle_ptr = ptr as *mut serde_json::map::Iter<'_>;

    if let Some(item) = (*handle_ptr).next() {
        let handle = Box::new(item);
        let handle_ptr = Box::into_raw(handle);

        return handle_ptr as *mut c_void;
    }
    else {
        return null_mut();
    }
}

#[no_mangle]
pub unsafe extern "C" fn serde_json_map_item_print(ptr: *mut c_void) {
    let handle_ptr = ptr as *mut <serde_json::map::Iter<'_> as Iterator>::Item;

    println!("{:?}", *handle_ptr);

}

#[no_mangle]
pub unsafe extern "C" fn serde_json_value_type(ptr: *mut c_void) -> JsonFieldType {
    let handle_ptr = Box::<serde_json::Value>::from_raw(ptr as *mut serde_json::Value);

    let value_type = match *handle_ptr {
        serde_json::Value::Null => JsonFieldType::Null,
        serde_json::Value::Bool(_) => JsonFieldType::Boolean,
        serde_json::Value::Number(_) => JsonFieldType::Float,
        serde_json::Value::String(_) => JsonFieldType::String,
        serde_json::Value::Array(_) => JsonFieldType::Array,
        serde_json::Value::Object(_) => JsonFieldType::Object,
    };

    let handle_ptr = Box::<serde_json::Value>::into_raw(handle_ptr);

    return value_type;
}

#[no_mangle]
pub unsafe extern "C" fn serde_json_vec_new() -> *mut c_void {
    let handle = Box::new(Vec::<serde_json::Value>::new());
    let handle_ptr = Box::<Vec<serde_json::Value>>::into_raw(handle);

    return handle_ptr as *mut c_void;
}

#[no_mangle]
pub unsafe extern "C" fn serde_json_vec_drop(ptr: *mut c_void) {
    let handle_ptr = Box::<Vec<serde_json::Value>>::from_raw(ptr as *mut Vec<serde_json::Value>);
}

#[no_mangle]
pub unsafe extern "C" fn serde_json_value_drop(ptr: *mut c_void) {
    let handle_ptr = ptr as *mut serde_json::Value;
    let handle = Box::<serde_json::Value>::from_raw(handle_ptr);
    println!("{:?}", handle);
}

#[no_mangle]
pub unsafe extern "C" fn serde_json_value_debug_print(ptr: *mut c_void) {
    let handle_ptr = ptr as *mut serde_json::Value;
    let handle = Box::<serde_json::Value>::from_raw(handle_ptr);
    println!("{:?}", handle);

    let handle_ptr = Box::<serde_json::Value>::into_raw(handle);
}

#[no_mangle]
pub unsafe extern "C" fn serde_json_map_debug_print(ptr: *mut c_void) {
    let handle_ptr = ptr as *mut serde_json::Map<String, serde_json::Value>;
    let handle = Box::<serde_json::Map<String, serde_json::Value>>::from_raw(handle_ptr);
    println!("{:?}", handle);

    let handle_ptr = Box::<serde_json::Map<String, serde_json::Value>>::into_raw(handle);
}

#[no_mangle]
pub unsafe extern "C" fn serde_json_vec_debug_print(ptr: *mut c_void) {
    let handle_ptr = ptr as *mut Vec<serde_json::Value>;
    let handle = Box::<Vec<serde_json::Value>>::from_raw(handle_ptr);
    println!("{:?}", handle);

    let handle_ptr = Box::<Vec<serde_json::Value>>::into_raw(handle);
}

#[no_mangle]
pub unsafe extern "C" fn serde_json_map2value(ptr: *mut c_void) -> *mut c_void {
    let handle_ptr = ptr as *mut serde_json::Map<String, serde_json::Value>;
    let handle = Box::<serde_json::Map<String, serde_json::Value>>::from_raw(handle_ptr);
    let value = serde_json::to_value(handle).unwrap();
    let value_ptr = Box::<serde_json::Value>::into_raw(Box::new(value));
    return value_ptr as *mut c_void;
}

#[no_mangle]
pub unsafe extern "C" fn serde_json_vec2value(ptr: *mut c_void) -> *mut c_void {
    let handle_ptr = ptr as *mut Vec<serde_json::Value>;
    let handle = Box::<Vec<serde_json::Value>>::from_raw(handle_ptr);
    let value = serde_json::to_value(handle).unwrap();
    let value_ptr = Box::<serde_json::Value>::into_raw(Box::new(value));
    return value_ptr as *mut c_void;
}

#[no_mangle]
pub unsafe extern "C" fn serde_json_i642value(val: i64) -> *mut c_void {
    let value = serde_json::to_value(val).unwrap();
    let value_ptr = Box::<serde_json::Value>::into_raw(Box::new(value));
    return value_ptr as *mut c_void;
}

#[no_mangle]
pub unsafe extern "C" fn serde_json_str2value(val: *const c_char) -> *mut c_void {
    let c_str: &CStr =  CStr::from_ptr(val) ;
    let str_slice: &str = c_str.to_str().unwrap();
    let value = serde_json::to_value(str_slice).unwrap();
    let value_ptr = Box::<serde_json::Value>::into_raw(Box::new(value));
    return value_ptr as *mut c_void;
}

#[no_mangle]
pub unsafe extern "C" fn serde_json_bool2value(val: bool) -> *mut c_void {
    let value = serde_json::to_value(val).unwrap();
    let value_ptr = Box::<serde_json::Value>::into_raw(Box::new(value));
    return value_ptr as *mut c_void;
}

#[no_mangle]
pub unsafe extern "C" fn serde_json_f642value(val: f64) -> *mut c_void {
    let value = serde_json::to_value(val).unwrap();
    let value_ptr = Box::<serde_json::Value>::into_raw(Box::new(value));
    return value_ptr as *mut c_void;
}

#[no_mangle]
pub unsafe extern "C" fn serde_json_null2value() -> *mut c_void {
    let value = serde_json::to_value(serde_json::Value::Null).unwrap();
    let value_ptr = Box::<serde_json::Value>::into_raw(Box::new(value));
    return value_ptr as *mut c_void;
}

#[no_mangle]
pub unsafe extern "C" fn serde_json_map_insert(map: *mut c_void, key: *const c_char, val: *mut c_void) -> *mut c_void {
    let value_ptr = val as *mut serde_json::Value;
    let value = Box::<serde_json::Value>::from_raw(value_ptr);
    let map_ptr = map as *mut serde_json::Map<String, serde_json::Value>;
    let c_str: &CStr =  CStr::from_ptr(key) ;
    let str_slice: &str = c_str.to_str().unwrap();
    
    (*map_ptr).insert(str_slice.to_string(), *value.clone());
    let map = Box::<serde_json::Map<String, serde_json::Value>>::from_raw(map_ptr);

    return Box::<serde_json::Map<String, serde_json::Value>>::into_raw(map) as *mut c_void;
}

#[no_mangle]
pub unsafe extern "C" fn serde_json_vec_push(vec: *mut c_void, val: *mut c_void) -> *mut c_void {
    let value_ptr = val as *mut serde_json::Value;
    let value = Box::<serde_json::Value>::from_raw(value_ptr);
    let vec_ptr = vec as *mut Vec<serde_json::Value>;
    (*vec_ptr).push(*value.clone());
    let vec = Box::<Vec<serde_json::Value>>::from_raw(vec_ptr);
    return Box::<Vec<serde_json::Value>>::into_raw(vec) as *mut c_void;
}

extern "C" {
    fn test_print();
    fn create_json() -> *mut c_void;
    fn free_json(ptr: *mut c_void);


    fn print_json(ptr: *mut c_void);

    fn insert_i64(ptr: *mut c_void, key: *const c_char, value: i64);


    fn insert_f64(ptr: *mut c_void, key: *const c_char, value: f64);

    fn insert_null(ptr: *mut c_void, key: *const c_char);

    fn insert_bool(ptr: *mut c_void, key: *const c_char, value: bool);

    fn insert_str(ptr: *mut c_void, key: *const c_char, value: *const c_char);

    fn create_json_array() -> *mut c_void;

    fn array_push_i64(ptr: *mut c_void, value: i64);

    fn array_push_f64(ptr: *mut c_void, value: f64);

    fn array_push_null(ptr: *mut c_void);

    fn array_push_bool(ptr: *mut c_void, value: bool);

    fn array_push_str(ptr: *mut c_void, value: *const c_char);

    fn array_push_obj(ptr: *mut c_void, obj: *mut c_void);

    fn insert_obj(ptr: *mut c_void, key: *const c_char, obj: *mut c_void);
}


unsafe fn serde_json2cpp_json(json: &serde_json::Value) -> *mut c_void {
    let result = create_json();

    for (key, value) in json.as_object().unwrap().iter() {
        let key_c_str = CString::new(key.as_str()).unwrap();
        let key_c_str_ptr: *const c_char = key_c_str.as_ptr() as *const c_char;

        match value {
            serde_json::Value::Null => {
                insert_null(result, key_c_str_ptr);
            },
            serde_json::Value::Bool(val) => {
                insert_bool(result, key_c_str_ptr, *val);
            },
            serde_json::Value::Number(val) => {
                if let Some(i64val) = val.as_i64() {
                    insert_i64(result, key_c_str_ptr, i64val);
                } else if let Some(f64val) = val.as_f64() {
                    insert_f64(result, key_c_str_ptr, f64val);
                } else {
                    panic!("unsupported number type");
                }
            },
            serde_json::Value::String(val) => {
                let val_c_str = CString::new(val.as_str()).unwrap();
                let val_c_str_ptr: *const c_char = val_c_str.as_ptr() as *const c_char;
                insert_str(result, key_c_str_ptr, val_c_str_ptr);
            },
            serde_json::Value::Array(val) => {
                let vec_ptr = serde_json2cpp_array(val);

                insert_obj(result, key_c_str_ptr, vec_ptr);

                free_json(vec_ptr);
            },
            serde_json::Value::Object(val) => {
                let map_ptr = serde_json2cpp_json(&value);
                insert_obj(result, key_c_str_ptr, map_ptr);

                free_json(map_ptr);
            },
        }
    }

    return result;
}

unsafe fn serde_json2cpp_array(arr: &Vec<serde_json::Value>) -> *mut c_void {
    let result = create_json_array();

    for value in arr.iter() {
        match value {
            serde_json::Value::Null => {
                array_push_null(result);
            },
            serde_json::Value::Bool(val) => {
                array_push_bool(result, *val);
            },
            serde_json::Value::Number(val) => {
                if let Some(i64val) = val.as_i64() {
                    array_push_i64(result, i64val);
                } else if let Some(f64val) = val.as_f64() {
                    array_push_f64(result, f64val);
                } else {
                    panic!("unsupported number type");
                }
            },
            serde_json::Value::String(val) => {
                let val_c_str = CString::new(val.as_str()).unwrap();
                let val_c_str_ptr: *const c_char = val_c_str.as_ptr() as *const c_char;
                array_push_str(result, val_c_str_ptr);
            },
            serde_json::Value::Array(val) => {
                
                let vec_ptr = serde_json2cpp_array(val);

                array_push_obj(result, vec_ptr);

                free_json(vec_ptr);
            },
            serde_json::Value::Object(val) => {
                let map_ptr = serde_json2cpp_json(&value);
                array_push_obj(result, map_ptr);
                free_json(map_ptr);
            },
        }
    }

    return result;
}

#[no_mangle]
pub unsafe extern "C" fn call_cpp_test() {
    println!("call_cpp_test");
    test_print();


    println!("call_cpp_test craft a json from rs");
    let c_str = CString::new("test_field").unwrap();
    let c_world: *const c_char = c_str.as_ptr() as *const c_char;

    let json = json!({
        "test_field": "test_value",
        "test_field2": 2,
    });

    let json_ptr = serde_json2cpp_json(&json);

    print_json(json_ptr);

    free_json(json_ptr);
}