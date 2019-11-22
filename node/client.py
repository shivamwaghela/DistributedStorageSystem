import grpc
import logging

import machine_info
import greet_pb2
import greet_pb2_grpc


def greet():
    node_ip = machine_info.get_ip()
    node_port = 2750
    channel = grpc.insecure_channel(node_ip + ":" + str(node_port))
    stub = greet_pb2_grpc.GreeterStub(channel)
    response = stub.SayHello(greet_pb2.HelloRequest(name=machine_info.get_ip()))

    logger.info("Response from " + machine_info.get_ip() + ": " + response.message)


if __name__ == '__main__':
    logger = logging.getLogger(machine_info.get_ip())
    logger.setLevel(logging.DEBUG)
    logging.basicConfig(filename='client.log', filemode='w',
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    greet()
