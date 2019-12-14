from concurrent import futures
import grpc
import logging
import sys
import threading
import os
sys.path.append("../" + os.path.dirname(os.path.realpath(__file__)))
sys.path.append(os.path.dirname(os.path.realpath(__file__)) + '/utils/')
sys.path.append(os.path.dirname(os.path.realpath(__file__)) + '/generated/')

import globals
import connection

from node_position import NodePosition
import greet_pb2_grpc
import network_manager_pb2_grpc
from client import Client
from server import Greeter
from network_manager import NetworkManager
from pulse import Pulse


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=50))
    greet_pb2_grpc.add_GreeterServicer_to_server(Greeter(), server)
    network_manager_pb2_grpc.add_NetworkManagerServicer_to_server(NetworkManager(), server)
    server.add_insecure_port("[::]:" + str(globals.port))
    logger.info("Server starting at port " + str(globals.port))
    server.start()
    server.wait_for_termination()


if __name__ == "__main__":
    globals.init()

    logging.basicConfig(filename='node.log', filemode='w',
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)

    if len(sys.argv) == 3:
        logger.info("Starting first node of the network at position: ({},{})"
                    .format(sys.argv[1], sys.argv[2]))
        my_node_coordinates = (int(sys.argv[1]), int(sys.argv[2]))
        my_pos = my_node_coordinates
        conn = connection.Connection(channel=None, node_position=NodePosition.CENTER,
                                     node_coordinates=my_node_coordinates, node_ip=globals.my_ip)
        globals.node_connections.add_connection(conn)
        logger.debug("NodeConnections.connection_dict: {}".format(globals.node_connections.connection_dict))

        server_thread = threading.Thread(target=serve)
        pulse_thread = threading.Thread(target=Pulse.check_neighbor_node_pulse)

        logger.debug("Starting server thread...")
        server_thread.start()

        logger.debug("Starting pulse thread...")
        pulse_thread.start()

        server_thread.join()
    else:
        if len(sys.argv) != 2:
            print("usage: python3 node/node.py [ipv4 address]")
            exit(1)

        client_thread = threading.Thread(target=Client.greet, args=(sys.argv[1],))
        server_thread = threading.Thread(target=serve)
        pulse_thread = threading.Thread(target=Pulse.check_neighbor_node_pulse)

        logger.debug("Starting client thread with target greet...")
        client_thread.start()

        logger.debug("Starting server thread with target serve...")
        server_thread.start()

        logger.debug("Starting server thread with target serve...")
        pulse_thread.start()

        server_thread.join()
