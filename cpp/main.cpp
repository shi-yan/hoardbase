#include <hoardbase.h>
#include <iostream>
#include "serde_json.h"
#include <nlohmann/json.hpp>

int main()
{
    void *db = hoardbase_open("test.db");
    /*std::cout << "Hello, World!" << std::endl;

    SerdeJsonMap map;
    map.init();

    nlohmann::json j2 = {
        {"pi", 3.141},
        {"happy", true},
        {"name", "Niels"},
        {"nothing", nullptr},
        {"answer", {{"everything", 42}}},
        {"list", {1, 0, 2}},
        {"object", {{"currency", "USD"}, {"value", 42.99}}}};

    map.from_json(j2);

    map.debug_print_cpp();

    map.debug_print();

    nlohmann::json j3;

    map.to_json(j3);

    std::cout << j3.dump(4) << std::endl;
*/
    hoardbase_create_collection(db, "test");


   // close(db);


}