#include <cstdarg>
#include <cstdint>
#include <cstdlib>
#include <ostream>
#include <new>

extern "C" {

void sixtyfps_shared_vector_free(uint8_t *ptr, uintptr_t size, uintptr_t align);

uint8_t *sixtyfps_shared_vector_allocate(uintptr_t size, uintptr_t align);

} // extern "C"
