import os
import sys
sys.path.append("../" + os.path.dirname(os.path.realpath(__file__)))
sys.path.append(os.path.dirname(os.path.realpath(__file__)) + '/utils/')
sys.path.append(os.path.dirname(os.path.realpath(__file__)) + '/generated/')

import globals
import connection
import grpc
import helper
import logging
import greet_pb2
import greet_pb2_grpc
import network_manager_pb2
import network_manager_pb2_grpc

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class Client:

    @staticmethod
    def greet(server_node_ip):
        """
        This method is used to get connected/added to the network.
        :param server_node_ip: ip address of any node in the network.
        :return: None
        """
        logger.info("Connecting to {} at port {}...".format(server_node_ip, globals.port))
        channel = grpc.insecure_channel(server_node_ip + ":" + str(globals.port))
        greeter_stub = greet_pb2_grpc.GreeterStub(channel)
        response = greeter_stub.SayHello(greet_pb2.HelloRequest(client_node_ip=globals.my_ip))

        logger.info("Response from {}: {}".format(server_node_ip, response))
        if eval(response.client_node_coordinates) is None:
            logger.error("Cannot join {}. Maximum node connection capacity reached or node already in the network."
                         .format(server_node_ip))
            return

        # Get coordinates => tuple(x,y)
        globals.my_coordinates = eval(response.client_node_coordinates)

        # Add yourself to NodeConnections
        my_conn = connection.Connection(channel=None,
                                        node_position=globals.my_position,
                                        node_coordinates=globals.my_coordinates,
                                        node_ip=globals.my_ip)
        globals.node_connections.add_connection(my_conn)
        logger.debug("NodeConnections.connection_dict: {}".format(globals.node_connections.connection_dict))

        # Calculate server node's position
        server_node_coordinates = eval(response.server_node_coordinates)

        neighbor_pos_coord_dict = helper.get_neighbor_coordinates(globals.my_coordinates)
        logger.debug("neighbor_pos_coord_dict: {}".format(neighbor_pos_coord_dict))

        server_node_position = None
        for item in neighbor_pos_coord_dict.items():
            if item[1] == server_node_coordinates:
                server_node_position = item[0]  # eg.: NodePosition.TOP
                break
        logger.debug(("server_node_position: {}".format(server_node_position)))

        # Add server node to NodeConnections
        server_node_conn = connection.Connection(channel=channel,
                                                 node_position=server_node_position,
                                                 node_coordinates=server_node_coordinates,
                                                 node_ip=server_node_ip)
        globals.node_connections.add_connection(server_node_conn)
        logger.debug("node_connections: {}".format(globals.node_connections.connection_dict))

        additional_connections = eval(response.additional_connections)
        logger.debug("additional_connections: {}".format(additional_connections))

        # If the new node (this node) has neighbors then make additional connections with them
        for server_node_ip in additional_connections:
            logger.info("Making necessary additional connections...")
            channel = grpc.insecure_channel(server_node_ip + ":" + str(globals.port))
            logger.info("Connecting to {} at port {}...".format(server_node_ip, globals.port))

            logger.info("Informing new neighbors to form connections...")
            logger.debug("Calling rpc UpdateNeighborMetaData with args: client_node_ip {}, client_node_coordinates: {}"
                         .format(globals.my_ip, globals.my_coordinates))
            network_manager_stub = network_manager_pb2_grpc.NetworkManagerStub(channel)
            response = network_manager_stub.UpdateNeighborMetaData(
                network_manager_pb2.UpdateNeighborMetaDataRequest(client_node_ip=globals.my_ip,
                                                                  client_node_coordinates=str(globals.my_coordinates)))

            logger.info("Response from {}: {}".format(server_node_ip, response))

            server_node_coordinates = eval(response.server_node_coordinates)
            server_node_position = None
            for item in neighbor_pos_coord_dict.items():
                if item[1] == server_node_coordinates:
                    server_node_position = item[0]
                    break

            # Add server node to NodeConnections
            server_node_conn = connection.Connection(channel=channel,
                                                     node_position=server_node_position,
                                                     node_coordinates=server_node_coordinates,
                                                     node_ip=server_node_ip)
            globals.node_connections.add_connection(server_node_conn)
            logger.debug("node_connections: {}".format(globals.node_connections.connection_dict))
        logger.info("Node added to the network successfully...")
        logger.info("Node details: node_coordinates: {}, node_connections: {}"
                    .format(globals.node_connections.connection_dict[globals.my_position].node_coordinates,
                            globals.node_connections.connection_dict))
