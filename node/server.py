from concurrent import futures
from yaml import load, Loader


import grpc
import logging
import os
import sys
sys.path.append("../" + os.path.dirname(os.path.realpath(__file__)))
sys.path.append(os.path.dirname(os.path.realpath(__file__)) + '/generated/')


import machine_info
import greet_pb2
import greet_pb2_grpc
import network_manager_pb2
import network_manager_pb2_grpc


connection_list = []


class Greeter(greet_pb2_grpc.GreeterServicer):

    def SayHello(self, request, context):
        logger.info("Greetings received from " + request.name)
        if request.name not in connection_list:
            connection_list.append(request.name)
        return greet_pb2.HelloReply(message='Hello, %s!' % request.name)


class NetworkManager(network_manager_pb2_grpc.NetworkManagerServicer):

    def GetConnectionList(self, request, context):
        logger.info("GetConnectionList called from: " + request.node_ip)
        return network_manager_pb2.GetConnectionListResponse(node_ip=connection_list)


def serve():
    config = load(open('config.yaml'), Loader=Loader)
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=50))
    greet_pb2_grpc.add_GreeterServicer_to_server(Greeter(), server)
    network_manager_pb2_grpc.add_NetworkManagerServicer_to_server(NetworkManager(), server)
    server.add_insecure_port(machine_info.get_ip() + ":" + str(config["port"]))
    logger.info("Server starting at port " + str(config["port"]))
    server.start()
    server.wait_for_termination()


if __name__ == "__main__":
    logging.basicConfig(filename='server.log', filemode='w',
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(machine_info.get_ip())
    logger.setLevel(logging.DEBUG)
    serve()
