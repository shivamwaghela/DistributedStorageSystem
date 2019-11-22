## Generate grpc modules for python3

```python3 -m grpc_tools.protoc -I./protos --python_out=./node/generated --grpc_python_out=./node/generated ./protos/greet.proto```