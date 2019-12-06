import grpc
import logging
import sys
import os
from yaml import load, Loader
sys.path.append("../" + os.path.dirname(os.path.realpath(__file__)))
sys.path.append(os.path.dirname(os.path.realpath(__file__)) + '/generated/')
from collections import deque
import rumour_pb2
import rumour_pb2_grpc
import time

node_port = 3000
gossip_queue = deque()
channels = []
file_hash_dict = {}
def sendRumour(channel, fileobj):
    rumour_stub = rumour_pb2_grpc.RumourStub(channel)
    rumour_stub.sendMyData(rumour_pb2.MyDataRequest(hash=fileobj.hash,
                                                            data=fileobj.data,
                                                            memory_usage=fileobj.memory_usage,
                                                            cpu_usage=fileobj.cpu_usage,
                                                            disk_usage=fileobj.disk_usage,
                                                            my_ip=fileobj.my_ip,
                                                            my_pos=fileobj.my_pos
                                                            ))
def receiveRumour(channel):
    rumour_stub = rumour_pb2_grpc.RumourStub(channel)
    response = rumour_stub.receiveFileData(rumour_pb2.FileDataRequest())
    if response.hash not in file_hash_dict:
        gossip_queue.append(response)
        file_hash_dict.append(response)

if __name__ == '__main__':
    channels.append(grpc.insecure_channel("10.0.0.1" + ":" + str(node_port)))
    channels.append(grpc.insecure_channel("10.0.0.2" + ":" + str(node_port)))
    channels.append(grpc.insecure_channel("10.0.0.3" + ":" + str(node_port)))
    channels.append(grpc.insecure_channel("10.0.0.4" + ":" + str(node_port)))
    while True:
        time.sleep(5)
        for fileobj in gossip_queue:
            for channel in channels:
                sendRumour(channel, fileobj)




