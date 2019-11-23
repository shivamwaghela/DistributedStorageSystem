import grpc
import logging
import sys
import os
sys.path.append("../" + os.path.dirname(os.path.realpath(__file__)))
sys.path.append(os.path.dirname(os.path.realpath(__file__)) + '/generated/')

import machine_info
import greet_pb2
import greet_pb2_grpc
import network_manager_pb2
import network_manager_pb2_grpc


def greet():
    greeter_stub = greet_pb2_grpc.GreeterStub(channel)
    response = greeter_stub.SayHello(greet_pb2.HelloRequest(name=machine_info.get_ip()))
    logger.info("Response from " + machine_info.get_ip() + ": " + response.message)
    print("Response from " + machine_info.get_ip() + ": " + response.message)


def get_connection_list():
    network_manager_stub = network_manager_pb2_grpc.NetworkManagerStub(channel)
    response = network_manager_stub.GetConnectionList(network_manager_pb2
                                                      .GetConnectionListRequest(node_ip=machine_info.get_ip()))
    print(response.node_ip)


if __name__ == '__main__':
    logging.basicConfig(filename='client.log', filemode='w',
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(machine_info.get_ip())
    logger.setLevel(logging.DEBUG)
    node_ip = machine_info.get_ip()
    node_port = 2750
    channel = grpc.insecure_channel(node_ip + ":" + str(node_port))
    greet()
    get_connection_list()
