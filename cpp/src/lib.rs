use std::ptr::null_mut;
use libc::c_char;
use std::ffi::CString;
use std::ffi::CStr;
use std::os::raw::{c_int, c_void};
use serde_json::Value;
use serde_json::Map;
use std::borrow::Borrow;


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

    let handle = Box::new(hoardbase::database::Database::open(&config).unwrap());
    let handle_ptr = Box::<hoardbase::database::Database>::into_raw(handle);            
    println!("created handle");

    return handle_ptr as *mut c_void;
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

#[no_mangle]
pub unsafe extern "C" fn serde_json_map_key(ptr: *mut c_void, index: usize) -> CString {
    let handle_ptr = Box::<serde_json::Map<String, serde_json::Value>>::from_raw(ptr as *mut serde_json::Map<String, serde_json::Value>);

    let mut keys = handle_ptr.keys();

    let c_str = keys.nth(index).unwrap().as_str();

    let c_string = CString::new(&c_str[..]).unwrap() ;
    
    return c_string;
}

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