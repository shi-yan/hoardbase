
cmake_minimum_required(VERSION 3.16)

project(Hoardbase)

include(FetchContent)

FetchContent_Declare(
    Corrosion
    GIT_REPOSITORY https://github.com/AndrewGaspar/corrosion.git
    GIT_TAG origin/master # Optionally specify a version tag or branch here
)

FetchContent_MakeAvailable(Corrosion)
#find_package(Corrosion REQUIRED)

FetchContent_Declare(
    json
    GIT_REPOSITORY https://github.com/nlohmann/json.git
    GIT_TAG origin/master # Optionally specify a version tag or branch here
)

FetchContent_MakeAvailable(json)


# Expose the crates as cmake target
corrosion_import_crate(
  MANIFEST_PATH "${CMAKE_CURRENT_SOURCE_DIR}/Cargo.toml"
  CRATES hoardbase_cpp
)

set(CMAKE_CXX_STANDARD 17) 
# Generate headers with cbindgen
set(generated_headers
  ${CMAKE_CURRENT_BINARY_DIR}/gen/hoardbase.h
)
file(GLOB generated_headers_dependencies
   "${CMAKE_CURRENT_SOURCE_DIR}/src/*.rs")
add_custom_target(
  generated_headers_target
  COMMAND cbindgen --crate hoardbase_cpp --output
        "${CMAKE_CURRENT_BINARY_DIR}/gen/hoardbase.h"
  BYPRODUCTS ${generated_headers}
  DEPENDS ${generated_headers_dependencies}
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)

add_library(hoardbase_cpp_rust_helper STATIC
  rust_helper.cpp
  record.cpp
)

target_include_directories(hoardbase_cpp_rust_helper PRIVATE
  ${json_SOURCE_DIR}/single_include
)

add_dependencies(hoardbase_cpp hoardbase_cpp_rust_helper)


# Create a target which we will export
add_library(hoardbase STATIC 
  ${CMAKE_CURRENT_SOURCE_DIR}/serde_json.cpp
)

target_include_directories(hoardbase PRIVATE
  ${CMAKE_CURRENT_BINARY_DIR}/gen
  ${json_SOURCE_DIR}/single_include
)

target_link_libraries(hoardbase PRIVATE hoardbase_cpp )

add_dependencies(hoardbase generated_headers_target)

# This will install the headers
set_property(TARGET hoardbase PROPERTY
  PUBLIC_HEADER include/hoardbase.h ${generated_headers}
)

add_executable(hoardbase_test
  ${CMAKE_CURRENT_SOURCE_DIR}/main.cpp
)

target_link_libraries(hoardbase_test PRIVATE hoardbase)


target_include_directories(hoardbase_test PRIVATE
  ${CMAKE_CURRENT_BINARY_DIR}/gen
  ${json_SOURCE_DIR}/single_include
)

add_dependencies(hoardbase_test hoardbase)


# export and install the SixtyFPS target so it can be used
#export(TARGETS hoardbase hoardbase_cpp
  #...
#)
#install( #... )