rm -rf build
mkdir -p build && cd build
cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_EXPORT_COMPILE_COMMANDS=1 .. && cmake --build .
