from concurrent import futures
from yaml import load, Loader

import grpc
import logging

import machine_info
import greet_pb2
import greet_pb2_grpc


class Greeter(greet_pb2_grpc.GreeterServicer):

    def SayHello(self, request, context):
        logger.info("Greetings received from " + request.name)
        return greet_pb2.HelloReply(message='Hello, %s!' % request.name)


def serve():
    config = load(open('../config.yaml'), Loader=Loader)
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=50))
    greet_pb2_grpc.add_GreeterServicer_to_server(Greeter(), server)
    server.add_insecure_port(machine_info.get_ip() + ":" + str(config["port"]))
    logger.info("Server starting at port " + str(config["port"]))
    server.start()
    server.wait_for_termination()


if __name__ == "__main__":
    logger = logging.getLogger(machine_info.get_ip())
    logger.setLevel(logging.DEBUG)
    logging.basicConfig(filename='server.log', filemode='w', format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    serve()
