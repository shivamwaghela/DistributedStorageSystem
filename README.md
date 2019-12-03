# Distributed Storage System
## Architecture
![](./docs/mesh.png)
## Installation and Setup
#### Prerequisites
- Python 3
#### Clone the repository
```bash
git clone https://github.com/shivamwaghela/DistributedStorageSystem.git
```
#### Go to the project directory
```bash
cd DistributedStorageSystem
```
#### Give execute permissions to shell scripts
```bash
chmod 744 install_modules.sh generate_grpc_code.sh
```
#### Install modules
```bash
./install_modules.sh
```
#### Generate gRPC code
```bash
./generate_grpc_code.sh
```
#### Run Server
```bash
python3 node/server.py
```
#### Run Client
```bash
python3 node/client.py
```
## Steps to bring up the network
#### Start the first node of the network
```
python3 node/server.py 0 0
```
#### Adding nodes to the network
##### Start client and connect to any node in the network
```
python3 node/client.py 10.0.0.1
```
##### Start server after node is into the network
```
python3 node/server.py
```
