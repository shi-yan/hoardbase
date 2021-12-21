#include <hoardbase.h>
#include <iostream>

int main()
{
    void *db = open("test.db");
    std::cout << "Hello, World!" << std::endl;
}