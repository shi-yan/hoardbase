#include <cstdint>
#include <nlohmann/json.hpp>
#include <string>
#include <chrono>

extern "C"
{
    void test_print();

    void* create_json();

    void free_json(void *json_ptr);

    void print_json(void *json_ptr);

    void insert_i64(void *json_ptr, const char *key, int64_t value);

    void insert_f64(void *json_ptr, const char *key, double value);

    void insert_null(void *json_ptr, const char *key);

    void insert_bool(void *json_ptr, const char *key, bool value);

    void insert_str(void *json_ptr, const char *key, const char *value);

    void* create_json_array();

    void array_push_i64(void *json_ptr, int64_t value);

    void array_push_f64(void *json_ptr, double value);

    void array_push_null(void *json_ptr);

    void array_push_bool(void *json_ptr, bool value);

    void array_push_str(void *json_ptr, const char *value);

    void array_push_obj(void *json_ptr, void *obj);

    void insert_obj(void *json_ptr, const char *key, void *obj);


    void* create_cpp_record(int64_t id, void *json_ptr, const char* hash, uint64_t last_modified);

}