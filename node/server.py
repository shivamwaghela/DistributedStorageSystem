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
import time

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

#file = open("node_metadata.txt", "w+")

node_meta_dict = {}
my_pos = (0, 0)


class Greeter(greet_pb2_grpc.GreeterServicer):

    def SayHello(self, request, context):
        global connection_dict, my_pos, node_meta_dict
        file = open("node_meta.txt", "r")
        node_meta_dict = eval(file.readlines()[0])
        file.close()
        # find our position
        my_ip = machine_info.get_ip()
        for pos in node_meta_dict:
            if node_meta_dict[pos] == my_ip:
                my_pos = pos
                break

        logger.info("Greetings received from " + request.name)
        file = open("connection_info.txt", "r")
        connection_dict = eval(file.readlines()[0])
        file.close()

        file = open("connection_info.txt", "w")
        connection_dict[request.name] = {"cpu_usage": request.cpu_usage,
                                         "memory_usage": request.memory_usage,
                                         "disk_usage": request.disk_usage}
        file.write(str(connection_dict))
        file.close()

        # figure out your available positions
        x, y = my_pos
        top = (x-1, y)
        bottom = (x+1, y)
        left = (x, y-1)
        right = (x, y+1)
        neighbor_pos = {"top": top, "bottom": bottom, "left": left, "right": right}
        available_pos = {}
        unavailable_pos = {}

        for pos in neighbor_pos:
            if neighbor_pos[pos] not in node_meta_dict.keys():
                available_pos[pos] = neighbor_pos[pos]
            else:
                unavailable_pos[pos] = neighbor_pos[pos]

        # TODO: if available_pos == 0 forward the request
        if len(available_pos) == 0:
            return greet_pb2.HelloReply(message='Hello, %s!' % request.name, my_pos="",
                                        your_pos="")

        # find position such that it maintains symmetry; check neighbor or neighbor's neighbor
        if len(node_meta_dict.keys()) == 1:
            new_node_pos = available_pos["right"]  # default to right position
            node_meta_dict[new_node_pos] = request.name
            file = open("node_meta.txt", "w+")
            file.write(str(node_meta_dict))
            file.close()
            return greet_pb2.HelloReply(message='Hello, %s!' % request.name, my_pos=str(my_pos),
                                        your_pos=str(new_node_pos))

        # eliminate farthest position
        if len(available_pos) == 3:
            if "top" in unavailable_pos and top == unavailable_pos["top"]:
                del available_pos["bottom"]
            elif "bottom" in unavailable_pos and bottom == unavailable_pos["bottom"]:
                del available_pos["top"]
            elif "left" in unavailable_pos.keys() and left == unavailable_pos["left"]:
                del available_pos["right"]
            elif "right" in unavailable_pos and left == unavailable_pos["right"]:
                del available_pos["left"]

        new_node_pos = ()

        # eliminate one more
        if len(available_pos) == 2:
            # now you have two options - L/R or T/B

            # get neighbor's node metadata
            my_ip = machine_info.get_ip()
            my_neighbor_pos = ()
            my_neighbor_ip = ""

            # TODO: gets only one neighbor pos; last neighbor
            for d in node_meta_dict.items():
                if d[1] != my_ip:
                    my_neighbor_pos = d[0]
                    my_neighbor_ip = d[1]

            node_ip = my_neighbor_ip
            node_port = 2750
            channel = grpc.insecure_channel(node_ip + ":" + str(node_port))
            network_manager_stub = network_manager_pb2_grpc.NetworkManagerStub(channel)
            response = network_manager_stub.GetNodeMetaData(network_manager_pb2.GetConnectionListRequest(
                                                            node_ip=machine_info.get_ip()))
            logger.info("GetNodeMetaData response from " + node_ip + " " + response.node_meta_dict)
            neighbor_meta_dict = eval(response.node_meta_dict)

            # check your neighbor's connections
            # assign the same position as your neighbor's neighbor to the new node
            # L->L else R; T->T else B

            x, y = my_neighbor_pos
            neighbor_top = (x - 1, y)
            neighbor_bottom = (x + 1, y)
            neighbor_left = (x, y - 1)
            neighbor_right = (x, y + 1)

            if "top" in available_pos and neighbor_top in neighbor_meta_dict:
                    new_node_pos = available_pos["top"]
            if "bottom" in available_pos and neighbor_bottom in neighbor_meta_dict:
                    new_node_pos = available_pos["bottom"]
            if "left" in available_pos and neighbor_left in neighbor_meta_dict:
                    new_node_pos = available_pos["left"]
            if "right" in available_pos and neighbor_right in neighbor_meta_dict:
                    new_node_pos = available_pos["right"]

        if new_node_pos == ():
            # assign random position
            new_node_pos = available_pos[random.choice(list(available_pos.keys()))]


        # assign node a position
        # send my position and the added node's position
        node_meta_dict[new_node_pos] = request.name
        file = open("node_meta.txt", "w+")
        file.write(str(node_meta_dict))
        file.close()

        # TODO: Also check if the new node has to make connections to its neighbors (if any)
        return greet_pb2.HelloReply(message='Hello, %s!' % request.name, my_pos=str(my_pos), your_pos=str(new_node_pos))


class NetworkManager(network_manager_pb2_grpc.NetworkManagerServicer):

    def GetConnectionList(self, request, context):
        logger.info("GetConnectionList called from: " + request.node_ip)
        return network_manager_pb2.GetConnectionListResponse(node_ip=str(connection_dict))

    def GetNodeMetaData(self, request, context):
        logger.info("GetNodeMetaData called from: " + request.node_ip)
        return network_manager_pb2.GetNodeMetaDataResponse(node_meta_dict="{(0,0): '10.10.10.10'}")



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
    print(sys.argv)
    if len(sys.argv) > 1 and sys.argv[1] == "0,0":
        my_pos = (0, 0)
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
            my_ip = machine_info.get_ip()
            for pos in node_meta_dict:
                if node_meta_dict[pos] == my_ip:
                    my_pos = pos
                    break
        except:
            node_meta_dict = {}
    serve()

## TODO: Store node metadata to file