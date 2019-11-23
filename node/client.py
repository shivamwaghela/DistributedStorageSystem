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

connection_list = []


def greet(ip, channel):
    greeter_stub = greet_pb2_grpc.GreeterStub(channel)
    response = greeter_stub.SayHello(greet_pb2.HelloRequest(name=machine_info.get_ip()))
    logger.info("Response from " + ip + ": " + response.message)


def get_connection_list():
    global connection_list
    network_manager_stub = network_manager_pb2_grpc.NetworkManagerStub(channel)
    response = network_manager_stub.GetConnectionList(network_manager_pb2
                                                      .GetConnectionListRequest(node_ip=machine_info.get_ip()))
    connection_list = response.node_ip


def greet_the_team():
    for ip in connection_list:
        if ip != machine_info.get_ip():
            chn = grpc.insecure_channel(ip + ":" + str(node_port))
            greet(ip, chn)


if __name__ == '__main__':
    logging.basicConfig(filename='client.log', filemode='w',
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(machine_info.get_ip())
    logger.setLevel(logging.DEBUG)
    node_ip = "10.0.0.2"
    node_port = 2750
    channel = grpc.insecure_channel(node_ip + ":" + str(node_port))
    greet(node_ip, channel)
    get_connection_list()
    greet_the_team()
