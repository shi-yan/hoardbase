use std::ptr::null_mut;
use libc::c_char;
use std::ffi::CString;
use std::ffi::CStr;
use std::os::raw::{c_int, c_void};


//https://sixtyfps.io/blog/expose-rust-library-to-other-languages.html

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
    let c_str: &CStr = unsafe { CStr::from_ptr(path) };
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
