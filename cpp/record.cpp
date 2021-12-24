#include "record.h"
#include <iostream>
#include <ctime>

void Record::debug_print() {
    std::cout << "id: " << this->id << std::endl;
    std::cout << "data: " << this->data << std::endl;
    std::cout << "hash: " << this->hash << std::endl;
    std::time_t epoch_time = std::chrono::system_clock::to_time_t(this->last_modified);
    std::cout << "last_modified: " << std::ctime(&epoch_time) << std::endl;
}