from concurrent import futures
import grpc
import logging
import sys
import threading
import os
sys.path.append("../" + os.path.dirname(os.path.realpath(__file__)))
sys.path.append(os.path.dirname(os.path.realpath(__file__)) + '/utils/')
sys.path.append(os.path.dirname(os.path.realpath(__file__)) + '/generated/')


import connection
import helper
import random
import node_connections
from node_position import NodePosition
import machine_info
import greet_pb2
import greet_pb2_grpc
import network_manager_pb2
import network_manager_pb2_grpc
import traversal_pb2.py
import traversal_pb2_grpc.py
import redis

#TODO - Replace with redis hostname, port, password 
r = redis.Redis(
    host='hostname',
    port=port, 
    password='password')

PORT = "2750"

## Client
def greet(ip, channel):
    global node_ip, node_port, my_ip
    greeter_stub = greet_pb2_grpc.GreeterStub(channel)
    response = greeter_stub.SayHello(greet_pb2.HelloRequest(name=machine_info.get_my_ip(),
                                                            cpu_usage=10.0,
                                                            memory_usage=10,
                                                            disk_usage=10))
    logger.info("Response from " + ip + ": " + str(response))
    if response.my_pos == "" and response.your_pos == "":
        logger.error("Cannot join " + ip)
        return
    node_meta_dict = {eval(response.my_pos): node_ip, eval(response.your_pos): machine_info.get_my_ip()}
    # file = open("node_meta.txt", "w+")
    # file.write(str(node_meta_dict))
    # file.close()


    my_conn = connection.Connection(channel=None, node_position=NodePosition.CENTER,
                                  node_coordinates=eval(response.your_pos), node_ip=my_ip)
    node_connections.add_connection(my_conn)

    # calc node_ip's position
    neighbor_pos_dict = helper.get_neighbor_coordinates(eval(response.your_pos))
    logger.info("neighbor_pos_dict: {}".format(neighbor_pos_dict))
    neighbor_pos = ()
    for item in neighbor_pos_dict.items():
        if item[1] == eval(response.my_pos): # my_pos => server's pos
            neighbor_pos = item[0] # NodePosition.TOP
            break

    logger.info(("neighbor_pos: {}".format(neighbor_pos)))
    neighbor_conn = connection.Connection(channel=channel, node_position=neighbor_pos,
                                          node_coordinates=eval(response.my_pos), node_ip=node_ip)
    node_connections.add_connection(neighbor_conn)

    logger.info("node_connections: {}".format(node_connections.connection_dict))

    connections_len = len(eval(response.additional_connections))

    if connections_len > 0:
        i = 0
        my_pos = response.your_pos
        my_ip = machine_info.get_my_ip()
        while i < connections_len:
            node_ip = eval(response.additional_connections)[i]
            channel = grpc.insecure_channel(node_ip + ":" + PORT)
            network_manager_stub = network_manager_pb2_grpc.NetworkManagerStub(channel)
            logger.info("greet: making add. conn. to " + node_ip)
            response = network_manager_stub.UpdateNeighborMetaData(
                network_manager_pb2.UpdateNeighborMetaDataRequest(node_meta_dict=str({eval(my_pos): my_ip})))
            logger.info("greet: response: " + str(response))
            node_meta_dict.update({eval(response.status): node_ip})
            neighbor_pos = ""
            for item in neighbor_pos_dict.items():
                if item[1] == eval(response.status):
                    neighbor_pos = item[0]  # NodePosition.TOP
                    break
            
            conn = connection.Connection(channel=channel, node_position=neighbor_pos, node_coordinates=eval(response.status), node_ip=node_ip)

            node_connections.add_connection(conn)
            logger.info("node_connections: {}".format(node_connections.connection_dict))
            # logger.info()
            i += 1


# Server
class Greeter(greet_pb2_grpc.GreeterServicer):

    def SayHello(self, request, context):
        global connection_dict, my_pos, node_meta_dict, my_ip
        my_pos = eval(str(node_connections.connection_dict[NodePosition.CENTER].node_coordinates))
        print(eval(str(my_pos)))
        # logger.info("my_pos", str(my_pos))
        logger.info("Greeter.SayHello: request: " + str(request.name))
        logger.info("Greeter.SayHello: reading node_meta.txt.")

        connection_dict = node_connections.connection_dict

        logger.info("Greeter.SayHello: read node_meta.txt; connection_dict: {}".format(connection_dict))

        # figure out your available positions
        neighbor_pos = helper.get_neighbor_coordinates(my_pos)
        logger.info("Greeter.SayHello: neighbor_pos: " + str(neighbor_pos))
        available_pos = {}
        unavailable_pos = {}

        # neighbor_pos = {NodePosition.TOP: top, NodePosition.BOTTOM: bottom, NodePosition.LEFT: left, NodePosition.RIGHT: right}

        for pos in neighbor_pos.keys():
            if pos in connection_dict.keys():
                unavailable_pos[pos] = neighbor_pos[pos]
            else:
                available_pos[pos] = neighbor_pos[pos]

        logger.info("Greeter.SayHello: available_pos: " + str(available_pos))
        logger.info("Greeter.SayHello: unavailable_pos: " + str(unavailable_pos))


        # TODO: if available_pos == 0 forward the request
        if len(available_pos) == 0:
            logger.info("Greeter.SayHello: len(available_pos) == 0")
            return greet_pb2.HelloReply(message='Hello, %s!' % request.name, my_pos="",
                                        your_pos="")

        # find position such that it maintains symmetry; check neighbor or neighbor's neighbor
        if len(available_pos) == 4:
            # logger.info("Greeter.SayHello: len(available_pos) == 4: " + str(node_meta_dict))
            new_node_pos = available_pos[NodePosition.RIGHT]  # default to right position
            channel = grpc.insecure_channel(request.name + ":2750")
            conn = connection.Connection(channel=channel, node_position=NodePosition.RIGHT,
                                         node_coordinates=new_node_pos, node_ip=request.name)
            node_connections.add_connection(conn)
            logger.info("node_connections: {}".format(node_connections.connection_dict))

            # logger.info("Greeter.SayHello:  Adding to default right: node_meta_dict:" + str(node_meta_dict))

            logger.info("Greeter.SayHello: Writing to node_meta_dict to file.")
            # file = open("node_meta.txt", "w")
            # file.write(str(node_meta_dict))
            # file.close()
            logger.info("Greeter.SayHello: Writing to node_meta_dict to file completed.")
            logger.info("Greeter.SayHello: response: additional_connections=[]")

            return greet_pb2.HelloReply(message='Hello, %s!' % request.name, my_pos=str(my_pos),
                                        your_pos=str(new_node_pos), additional_connections=str([]))

        # eliminate farthest position
        if len(available_pos) == 3:
            logger.info("Greeter.SayHello: len(available_pos) == 3; available_pos: (before)" + str(available_pos))
            if NodePosition.TOP in unavailable_pos \
                    and neighbor_pos[NodePosition.TOP] == unavailable_pos[NodePosition.TOP]:
                del available_pos[NodePosition.BOTTOM]
            elif NodePosition.BOTTOM in unavailable_pos \
                    and neighbor_pos[NodePosition.BOTTOM] == unavailable_pos[NodePosition.BOTTOM]:
                del available_pos[NodePosition.TOP]
            elif NodePosition.LEFT in unavailable_pos.keys() \
                    and neighbor_pos[NodePosition.LEFT] == unavailable_pos[NodePosition.LEFT]:
                del available_pos[NodePosition.RIGHT]
            elif NodePosition.RIGHT in unavailable_pos \
                    and neighbor_pos[NodePosition.RIGHT] == unavailable_pos[NodePosition.RIGHT]:
                del available_pos[NodePosition.LEFT]
            logger.info("Greeter.SayHello: len(available_pos) == 3; available_pos: (after)" + str(available_pos))

        new_node_pos = ()
        my_neighbors_neighbor_pos = {}
        neighbor_meta_dict = {}
        pos_direction = None
        my_neighbor_pos = ()
        my_neighbor_ip = ""

        # eliminate one more
        if len(available_pos) == 2:
            logger.info("Greeter.SayHello: len(available_pos) == 2; available_pos: (before)" + str(available_pos))
            # now you have two options - L/R or T/B
            # get neighbor's node metadata
            # TODO: gets only one neighbor pos; last neighbor
            # for d in node_meta_dict.items():
            #     if d[1] != my_ip:
            #         my_neighbor_pos = d[0]
            #         my_neighbor_ip = d[1]
            for my_neighbor_direction in unavailable_pos.keys():
                my_neighbor_pos = unavailable_pos[my_neighbor_direction] # (x,y)
                my_neighbor_ip = connection_dict[my_neighbor_direction].node_ip # ip
                logger.info("Greeter.SayHello: my_neighbor_pos: " + str(my_neighbor_pos))
                logger.info("Greeter.SayHello: my_neighbor_ip: " + str(my_neighbor_ip))

                node_ip = my_neighbor_ip
                channel = grpc.insecure_channel(node_ip + ":2750")
                network_manager_stub = network_manager_pb2_grpc.NetworkManagerStub(channel)

                logger.info("Greeter.SayHello: Request GetNodeMetaData in: " + node_ip)
                response = network_manager_stub.GetNodeMetaData(network_manager_pb2.GetConnectionListRequest(
                                                                node_ip=machine_info.get_my_ip()))
                #logger.info("Greeter.SayHello: GetNodeMetaData response from " + node_ip + " " + response.node_meta_dict)
                neighbor_meta_dict = eval(response.node_meta_dict)

                logger.info("Greeter.SayHello: neighbor_meta_dict: " + str(neighbor_meta_dict))
                # check your neighbor's connections
                # assign the same position as your neighbor's neighbor to the new node
                # L->L else R; T->T else B

                logger.info("Greeter.SayHello: my_neighbor_pos: " + str(my_neighbor_pos))
                my_neighbors_neighbor_pos = helper.get_neighbor_coordinates(my_neighbor_pos)

                logger.info("Greeter.SayHello: my_neighbors_neighbor_pos: " + str(my_neighbors_neighbor_pos))

                if NodePosition.TOP in available_pos \
                        and my_neighbors_neighbor_pos[NodePosition.TOP] in neighbor_meta_dict \
                        and neighbor_meta_dict[my_neighbors_neighbor_pos[NodePosition.TOP]] != my_ip:
                        new_node_pos = available_pos[NodePosition.TOP]
                        pos_direction = NodePosition.TOP
                if NodePosition.BOTTOM in available_pos \
                        and my_neighbors_neighbor_pos[NodePosition.BOTTOM] in neighbor_meta_dict \
                        and neighbor_meta_dict[my_neighbors_neighbor_pos[NodePosition.BOTTOM]] != my_ip:
                        new_node_pos = available_pos[NodePosition.BOTTOM]
                        pos_direction = NodePosition.BOTTOM
                if NodePosition.LEFT in available_pos \
                        and my_neighbors_neighbor_pos[NodePosition.LEFT] in neighbor_meta_dict \
                        and neighbor_meta_dict[my_neighbors_neighbor_pos[NodePosition.LEFT]] != my_ip:
                        new_node_pos = available_pos[NodePosition.LEFT]
                        pos_direction = NodePosition.LEFT
                if NodePosition.RIGHT in available_pos \
                        and my_neighbors_neighbor_pos[NodePosition.RIGHT] in neighbor_meta_dict \
                        and neighbor_meta_dict[my_neighbors_neighbor_pos[NodePosition.RIGHT]] != my_ip:
                        new_node_pos = available_pos[NodePosition.RIGHT]
                        pos_direction = NodePosition.RIGHT

                if pos_direction:
                    del available_pos[pos_direction]

                if new_node_pos != ():
                    break

            logger.info("Greeter.SayHello: new_node_pos: " + str(new_node_pos))
            logger.info("Greeter.SayHello: pos_direction: " + str(pos_direction))
            logger.info("Greeter.SayHello: len(available_pos) == 2; available_pos: (after)" + str(available_pos))

        if new_node_pos == ():
            logger.info("Greeter.SayHello: new_node_pos == () " + str(len(new_node_pos)))
            # assign random position
            random_pos = random.choice(list(available_pos.keys()))
            new_node_pos = available_pos[random_pos]
            pos_direction = random_pos
            logger.info("Greeter.SayHello: new_node_pos: " + str(new_node_pos))
            logger.info("Greeter.SayHello: pos_direction: " + str(pos_direction))

        # assign node a position
        # send my position and the added node's position

        try:
            additional_connections = str([neighbor_meta_dict[my_neighbors_neighbor_pos[pos_direction]]])
        except KeyError:
            additional_connections = str([])

        logger.info("Greeter.SayHello: additional_connections: " + str(additional_connections))
        logger.info("Greeter.SayHello: my_pos: " + str(my_pos))
        logger.info("Greeter.SayHello: new_node_pos: " + str(new_node_pos))

        channel = grpc.insecure_channel(request.name + ":2750")
        
        conn = connection.Connection(channel=channel, node_position=pos_direction,
                                      node_coordinates=new_node_pos, node_ip=request.name)

        node_connections.add_connection(conn)
        logger.info("node_connections: {}".format(node_connections.connection_dict))

        return greet_pb2.HelloReply(message='Hello, %s!' % request.name, my_pos=str(my_pos), your_pos=str(new_node_pos),
                                    additional_connections=additional_connections)


class NetworkManager(network_manager_pb2_grpc.NetworkManagerServicer):

    def GetNodeMetaData(self, request, context):
        logger.info("GetNodeMetaData called from: " + request.node_ip)
        # file = open("node_meta.txt", "r")
        # node_meta_dict = eval(file.readlines()[0])
        # file.close()
        node_meta_dict = {}
        for conn in node_connections.connection_dict.values():
            node_meta_dict[conn.node_coordinates] = conn.node_ip
        return network_manager_pb2.GetNodeMetaDataResponse(node_meta_dict=str(node_meta_dict))

    def UpdateNeighborMetaData(self, request, context):
        logger.info("NetworkManager.UpdateNeighborMetaData: request: " + str(request.node_meta_dict))
        logger.info("NetworkManager.UpdateNeighborMetaData: reading node_meta.txt: ")
        # file = open("node_meta.txt", "r")
        # node_meta_dict = eval(file.readlines()[0])
        # file.close()
        #logger.info("NetworkManager.UpdateNeighborMetaData: node_meta_dict: " + str(node_meta_dict))

        #logger.info("NetworkManager.UpdateNeighborMetaData: updating node_meta_dict: " + str(node_meta_dict))
        # file = open("node_meta.txt", "w")
        #node_meta_dict.update(eval(request.node_meta_dict))
        # file.write(str(node_meta_dict))
        # file.close()
        my_pos = node_connections.connection_dict[NodePosition.CENTER].node_coordinates
        print(type(my_pos))
        print(request.node_meta_dict)
        node_coord = list(eval(request.node_meta_dict).keys())[0]
        print(node_coord)
        neighbor_pos_dict = helper.get_neighbor_coordinates(my_pos)
        logger.info("neighbor_pos_dict: {}".format(neighbor_pos_dict))
        neighbor_pos = ()
        for item in neighbor_pos_dict.items():
            logger.info("Item {} {} {} {}".format(item[0], type(item[0]), item[1], type(item[1])))
            if item[1] == node_coord:  # my_pos => server's pos
                neighbor_pos = item[0]  # NodePosition.TOP
                break
        node_ip = eval(request.node_meta_dict)[node_coord]
        channel = grpc.insecure_channel(node_ip + ":2750")
        conn = connection.Connection(channel=channel, node_coordinates=node_coord, node_position=neighbor_pos, node_ip=node_ip)
        node_connections.add_connection(conn)
        logger.info("node_connections: {}".format(node_connections.connection_dict))
        # logger.info("NetworkManager.UpdateNeighborMetaData: node_meta_dict: " + str(node_meta_dict))
        # logger.info("NetworkManager.UpdateNeighborMetaData: my_pos: " + str(my_node_coordinates))

        return network_manager_pb2.UpdateNeighborMetaDataResponse(status=str(my_pos))


class TraversalServicer(traversal_pb2_grpc.TraversalServicer): 
    def ReceiveRequest(self, request, context):
        logger.info("Here")
        channel = grpc.insecure_channel('localhost:5555')
        fileServerStub = ../CMPE-275-MemoryStorage/src/chunk_pb2_grpc.FileServerServicer #placeholder to get william's code
        #Check if file is present on my node
        app_n = "dropbox_app"
        file_p = "data/test_in.txt"
        output_path = "data/test_out.txt"
        file_n = os.path.basename(file_p)
        currentFile = fileServerStub.download(app_n,  file_n, output_path)
        if currentFile != None:
            return currentFile
            # return traversal_pb2.GetReceiveResponse(file_bytes=file, request_id=request.request_id)
        else:
            #create request object
            curr_filename = request.filename
            curr_request_id = request.request_id
            curr_stack = request.stack
            curr_visited = request.visited
            # channel = grpc.insecure_channel('localhost:50051')
            # add neighbors to stack. before adding check if neighbor is already visited.
            for neighbor in connection_dict.items():
                if neighbor[1].node_ip in (eval(curr_visited)):
                    continue
                else:
                    curr_stack = eval(curr_stack)
                    curr_stack.append(neighbor[1])
                    # request_object.stack = curr_stack
            curr_stack_object = curr_stack.pop()
            stub = traversal_pb2_grpc.TraversalStub(curr_stack_object.channel)
            request_object = traversal_pb2.GetReceieveRequest(filename = curr_filename, request_id = curr_request_id, stack = curr_stack, visited = curr_visited)
            stub.ReceiveRequest(request_object)



def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=50))
    greet_pb2_grpc.add_GreeterServicer_to_server(Greeter(), server)
    network_manager_pb2_grpc.add_NetworkManagerServicer_to_server(NetworkManager(), server)
    traversal_pb2_grpc.add_TraversalServicer_to_server(TraversalServicer(), server)
    server.add_insecure_port("[::]:" + PORT)
    logger.info("Server starting at port " + PORT)
    server.start()
    server.wait_for_termination()


if __name__ == "__main__":
    my_ip = machine_info.get_my_ip()
    logging.basicConfig(filename='node.log', filemode='w',
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(my_ip)
    logger.setLevel(logging.DEBUG)

    node_connections = node_connections.NodeConnections()

    if len(sys.argv) == 3:
        logger.info("Starting first node of the network at position: ({},{})"
                    .format(sys.argv[1], sys.argv[2]))
        my_node_coordinates = (int(sys.argv[1]), int(sys.argv[2]))
        my_pos = my_node_coordinates
        conn = connection.Connection(channel=None, node_position=NodePosition.CENTER,
                                     node_coordinates=my_node_coordinates, node_ip=my_ip)
        node_connections.add_connection(conn)
        logger.info("node_connections: {}".format(node_connections.connection_dict))
        # logger.info("node_connections: {}".format(node_connections))
        server_thread = threading.Thread(target=serve)
        server_thread.start()
        server_thread.join()
    else:
        if len(sys.argv) != 2:
            print("usage: python3 node/client.py [ipv4 address]")
            exit(1)
        logger.info("Connecting to a node in network: {}".format(sys.argv[1]))
        node_ip = sys.argv[1]
        node_port = PORT
        channel = grpc.insecure_channel(node_ip + ":" + str(node_port))
        client_thread = threading.Thread(target=greet, args=(node_ip, channel))
        server_thread = threading.Thread(target=serve)
        traversal_thread = threading.Thread(target=ReceiveRequest, args=(request))
        client_thread.start()
        server_thread.start()
        server_thread.join()
