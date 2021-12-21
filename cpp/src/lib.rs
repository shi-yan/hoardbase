use std::ptr::null_mut;

//https://sixtyfps.io/blog/expose-rust-library-to-other-languages.html
#[repr(C)]
pub struct Database {

}

#[no_mangle]
pub unsafe extern "C" fn sixtyfps_shared_vector_free(
    ptr: *mut u8, size: usize, align: usize) { /*...*/
        
    }


#[no_mangle]
pub unsafe extern "C" fn sixtyfps_shared_vector_allocate(
    size: usize, align: usize) -> *mut u8 { /*...*/
        return null_mut();
    }