from concurrent import futures
from yaml import load, Loader
from psutil import cpu_percent, virtual_memory, disk_usage


import grpc
import logging
import os
import random
import sys
sys.path.append("../" + os.path.dirname(os.path.realpath(__file__)))
sys.path.append(os.path.dirname(os.path.realpath(__file__)) + '/generated/')
sys.path.append(os.path.dirname(os.path.realpath(__file__)) + '/utils/')

import helper
import machine_info
import greet_pb2
import greet_pb2_grpc
import network_manager_pb2
import network_manager_pb2_grpc
import machine_stats_pb2
import machine_stats_pb2_grpc

cpu_usage = machine_info.get_my_cpu_usage()
memory_usage = machine_info.get_my_memory_usage()
hdd_usage = machine_info.get_my_disk_usage()

connection_dict = {machine_info.get_ip(): {"cpu_usage": cpu_usage,
                                           "memory_usage": memory_usage,
                                           "disk_usage": hdd_usage}}
file = open("connection_info.txt", "w+")
file.write(str(connection_dict))
file.close()

node_meta_dict = {}
my_pos = (0, 0)


class Greeter(greet_pb2_grpc.GreeterServicer):

    def SayHello(self, request, context):
        global connection_dict, my_pos, node_meta_dict, my_ip
        logger.info("Greeter.SayHello: request: " + str(request.name))
        logger.info("Greeter.SayHello: reading node_meta.txt.")
        file = open("node_meta.txt", "r")
        node_meta_dict = eval(file.readlines()[0])
        file.close()

        logger.info("Greeter.SayHello: read node_meta.txt; node_meta_dict: " + str(node_meta_dict))

        # find our position
        logger.info("Greeter.SayHello: my_ip: " + my_ip)
        for pos in node_meta_dict.keys():
            if node_meta_dict[pos] == my_ip:
                my_pos = pos
                break
        logger.info("Greeter.SayHello: my_pos: " + str(my_pos))

        #file = open("connection_info.txt", "r")
        #connection_dict = eval(file.readlines()[0])
        #file.close()

        #file = open("connection_info.txt", "w")
        #connection_dict[request.name] = {"cpu_usage": request.cpu_usage,
        #                                 "memory_usage": request.memory_usage,
        #                                 "disk_usage": request.disk_usage}
        #file.write(str(connection_dict))
        #file.close()

        # figure out your available positions
        neighbor_pos = helper.get_neighbor_coordinates(my_pos)
        logger.info("Greeter.SayHello: neighbor_pos: " + str(neighbor_pos))
        available_pos = {}
        unavailable_pos = {}

        for pos in neighbor_pos.keys():
            if neighbor_pos[pos] not in node_meta_dict.keys():
                available_pos[pos] = neighbor_pos[pos]
            else:
                unavailable_pos[pos] = neighbor_pos[pos]

        logger.info("Greeter.SayHello: available_pos: " + str(available_pos))
        logger.info("Greeter.SayHello: unavailable_pos: " + str(unavailable_pos))


        # TODO: if available_pos == 0 forward the request
        if len(available_pos) == 0:
            logger.info("Greeter.SayHello: len(available_pos) == 0")
            return greet_pb2.HelloReply(message='Hello, %s!' % request.name, my_pos="",
                                        your_pos="")

        # find position such that it maintains symmetry; check neighbor or neighbor's neighbor
        if len(available_pos) == 4:
            logger.info("Greeter.SayHello: len(available_pos) == 4: " + str(node_meta_dict))
            new_node_pos = available_pos["right"]  # default to right position
            node_meta_dict[new_node_pos] = request.name

            logger.info("Greeter.SayHello:  Adding to default right: node_meta_dict:" + str(node_meta_dict))

            logger.info("Greeter.SayHello: Writing to node_meta_dict to file.")
            file = open("node_meta.txt", "w")
            file.write(str(node_meta_dict))
            file.close()
            logger.info("Greeter.SayHello: Writing to node_meta_dict to file completed.")
            logger.info("Greeter.SayHello: response: additional_connections=[]")

            return greet_pb2.HelloReply(message='Hello, %s!' % request.name, my_pos=str(my_pos),
                                        your_pos=str(new_node_pos), additional_connections=str([]))

        # eliminate farthest position
        if len(available_pos) == 3:
            logger.info("Greeter.SayHello: len(available_pos) == 3; available_pos: (before)" + str(available_pos))
            if "top" in unavailable_pos and neighbor_pos["top"] == unavailable_pos["top"]:
                del available_pos["bottom"]
            elif "bottom" in unavailable_pos and neighbor_pos["bottom"] == unavailable_pos["bottom"]:
                del available_pos["top"]
            elif "left" in unavailable_pos.keys() and neighbor_pos["left"] == unavailable_pos["left"]:
                del available_pos["right"]
            elif "right" in unavailable_pos and neighbor_pos["right"] == unavailable_pos["right"]:
                del available_pos["left"]
            logger.info("Greeter.SayHello: len(available_pos) == 3; available_pos: (after)" + str(available_pos))

        new_node_pos = ()
        my_neighbors_neighbor_pos = {}
        neighbor_meta_dict = {}
        pos_direction = ""
        my_neighbor_pos = ()
        my_neighbor_ip = ""

        # eliminate one more
        if len(available_pos) == 2:
            logger.info("Greeter.SayHello: len(available_pos) == 2; available_pos: (before)" + str(available_pos))
            # now you have two options - L/R or T/B
            # get neighbor's node metadata
            # TODO: gets only one neighbor pos; last neighbor
            for d in node_meta_dict.items():
                if d[1] != my_ip:
                    my_neighbor_pos = d[0]
                    my_neighbor_ip = d[1]
            logger.info("Greeter.SayHello: my_neighbor_pos: " + str(my_neighbor_pos))
            logger.info("Greeter.SayHello: my_neighbor_ip: " + str(my_neighbor_ip))

            node_ip = my_neighbor_ip
            node_port = 2750
            channel = grpc.insecure_channel(node_ip + ":" + str(node_port))
            network_manager_stub = network_manager_pb2_grpc.NetworkManagerStub(channel)

            logger.info("Greeter.SayHello: Request GetNodeMetaData in: " + node_ip)
            response = network_manager_stub.GetNodeMetaData(network_manager_pb2.GetConnectionListRequest(
                                                            node_ip=machine_info.get_ip()))
            logger.info("Greeter.SayHello: GetNodeMetaData response from " + node_ip + " " + response.node_meta_dict)
            neighbor_meta_dict = eval(response.node_meta_dict)

            logger.info("Greeter.SayHello: neighbor_meta_dict: " + str(neighbor_meta_dict))
            # check your neighbor's connections
            # assign the same position as your neighbor's neighbor to the new node
            # L->L else R; T->T else B

            logger.info("Greeter.SayHello: my_neighbor_pos: " + str(my_neighbor_pos))
            my_neighbors_neighbor_pos = helper.get_neighbor_coordinates(my_neighbor_pos)

            logger.info("Greeter.SayHello: my_neighbors_neighbor_pos: " + str(my_neighbors_neighbor_pos))

            if "top" in available_pos and my_neighbors_neighbor_pos["top"] in neighbor_meta_dict \
                    and neighbor_meta_dict["top"] != my_ip:
                    new_node_pos = available_pos["top"]
                    pos_direction = "top"
            if "bottom" in available_pos and my_neighbors_neighbor_pos["bottom"] in neighbor_meta_dict \
                    and neighbor_meta_dict["bottom"] != my_ip:
                    new_node_pos = available_pos["bottom"]
                    pos_direction = "bottom"
            if "left" in available_pos and my_neighbors_neighbor_pos["left"] in neighbor_meta_dict \
                    and neighbor_meta_dict["left"] != my_ip:
                    new_node_pos = available_pos["left"]
                    pos_direction = "left"
            if "right" in available_pos and my_neighbors_neighbor_pos["right"] in neighbor_meta_dict \
                    and neighbor_meta_dict["right"] != my_ip:
                    new_node_pos = available_pos["right"]
                    pos_direction = "right"

            logger.info("Greeter.SayHello: new_node_pos: " + str(new_node_pos))
            logger.info("Greeter.SayHello: pos_direction: " + str(pos_direction))
            logger.info("Greeter.SayHello: len(available_pos) == 2; available_pos: (after)" + str(available_pos))

        if new_node_pos == ():
            logger.info("Greeter.SayHello: new_node_pos == () " + str(len(new_node_pos)))
            # assign random position
            random_pos = random.choice(list(available_pos.keys()))
            new_node_pos = available_pos[random_pos]
            pos_direction = ""
            logger.info("Greeter.SayHello: new_node_pos: " + str(new_node_pos))
            logger.info("Greeter.SayHello: pos_direction: " + str(pos_direction))

        # assign node a position
        # send my position and the added node's position
        node_meta_dict[new_node_pos] = request.name
        logger.info("Greeter.SayHello: node_meta_dict: " + str(node_meta_dict))
        logger.info("Greeter.SayHello: writing node_meta_dict to node_meta.txt: " + str(node_meta_dict))
        file = open("node_meta.txt", "w")
        file.write(str(node_meta_dict))
        file.close()
        logger.info("Greeter.SayHello: writing node_meta_dict to node_meta.txt completed: " + str(node_meta_dict))

        additional_connections = str([neighbor_meta_dict[my_neighbors_neighbor_pos[pos_direction]]]) if pos_direction != "" \
                                    else str([])

        logger.info("Greeter.SayHello: additional_connections: " + str(additional_connections))
        logger.info("Greeter.SayHello: my_pos: " + str(my_pos))
        logger.info("Greeter.SayHello: new_node_pos: " + str(new_node_pos))

        return greet_pb2.HelloReply(message='Hello, %s!' % request.name, my_pos=str(my_pos), your_pos=str(new_node_pos),
                                    additional_connections=additional_connections)


class NetworkManager(network_manager_pb2_grpc.NetworkManagerServicer):

    def GetConnectionList(self, request, context):
        logger.info("GetConnectionList called from: " + request.node_ip)
        return network_manager_pb2.GetConnectionListResponse(node_ip=str(connection_dict))

    def GetNodeMetaData(self, request, context):
        logger.info("GetNodeMetaData called from: " + request.node_ip)
        file = open("node_meta.txt", "r")
        node_meta_dict = eval(file.readlines()[0])
        file.close()
        return network_manager_pb2.GetNodeMetaDataResponse(node_meta_dict=str(node_meta_dict))

    def UpdateNeighborMetaData(self, request, context):
        logger.info("NetworkManager.UpdateNeighborMetaData: request: " + str(request.node_meta_dict))
        logger.info("NetworkManager.UpdateNeighborMetaData: reading node_meta.txt: ")
        file = open("node_meta.txt", "r")
        node_meta_dict = eval(file.readlines()[0])
        file.close()
        logger.info("NetworkManager.UpdateNeighborMetaData: node_meta_dict: " + str(node_meta_dict))

        logger.info("NetworkManager.UpdateNeighborMetaData: updating node_meta_dict: " + str(node_meta_dict))
        file = open("node_meta.txt", "w")
        node_meta_dict.update(eval(request.node_meta_dict))
        file.write(str(node_meta_dict))
        file.close()
        logger.info("NetworkManager.UpdateNeighborMetaData: node_meta_dict: " + str(node_meta_dict))
        logger.info("NetworkManager.UpdateNeighborMetaData: my_pos: " + str(my_pos))

        return network_manager_pb2.UpdateNeighborMetaDataResponse(status=str(my_pos))


class MachineState(machine_stats_pb2_grpc.MachineStatsServicer):

    def GetCPUUsage(self, request, context):
        logger.info("GetCPUUsage called from: " + request.node_ip)
        cpu_usage = cpu_percent(interval=5)
        logger.info("CPU utilization: " + cpu_usage)
        return machine_stats_pb2.GetCPUUsageResponse(cpu_usage=cpu_usage)

    def GetMemoryUsage(self, request, context):
        logger.info("GetMemoryUsage called from: " + request.node_ip)
        memory_usage = virtual_memory()
        logger.info("Memory utilization: " + memory_usage)
        return machine_stats_pb2.GetMemoryUsageResponse(memory_usage=memory_usage)

    def GetDiskUsage(self, request, context):
        logger.info("GetDiskUsage called from: " + request.node_ip)
        hdd_usage = disk_usage("/")
        logger.info("Disk utilization: " + hdd_usage)
        return machine_stats_pb2.GetCPUUsageResponse(disk_usage=hdd_usage)


def serve():
    config = load(open('config.yaml'), Loader=Loader)
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=50))
    greet_pb2_grpc.add_GreeterServicer_to_server(Greeter(), server)
    network_manager_pb2_grpc.add_NetworkManagerServicer_to_server(NetworkManager(), server)
    server.add_insecure_port("[::]:" + str(config["port"]))
    logger.info("Server starting at port " + str(config["port"]))
    server.start()
    server.wait_for_termination()


if __name__ == "__main__":
    logging.basicConfig(filename='server.log', filemode='w',
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(machine_info.get_ip())
    logger.setLevel(logging.DEBUG)
    my_ip = machine_info.get_ip()
    if len(sys.argv) == 3:
        my_pos = (int(sys.argv[1]), int(sys.argv[2]))
        node_meta_dict[my_pos] = machine_info.get_ip()
        file = open("node_meta.txt",  "w+")
        file.write(str(node_meta_dict))
        file.close()
    else:
        try:
            file = open("node_meta.txt", "r")
            node_meta_dict = eval(file.readlines()[0])
            file.close()
            # find our position
            for pos in node_meta_dict:
                if node_meta_dict[pos] == my_ip:
                    my_pos = pos
                    break
        except:
            node_meta_dict = {}
            file = open("node_meta.txt", "w+")
            file.write(str(node_meta_dict))
            file.close()
    serve()
