from concurrent import futures
from yaml import load, Loader
from psutil import cpu_percent, virtual_memory, disk_usage


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


class Greeter(greet_pb2_grpc.GreeterServicer):

    def SayHello(self, request, context):
        global connection_dict
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
        return greet_pb2.HelloReply(message='Hello, %s!' % request.name)


class NetworkManager(network_manager_pb2_grpc.NetworkManagerServicer):

    def GetConnectionList(self, request, context):
        logger.info("GetConnectionList called from: " + request.node_ip)
        return network_manager_pb2.GetConnectionListResponse(node_ip=str(connection_dict))


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
    serve()
