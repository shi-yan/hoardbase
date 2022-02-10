#!/bin/bash

docker run --rm -v $(pwd):/io konstin2/maturin build --release -m python/Cargo.toml
