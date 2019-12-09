import grpc
import logging
import sys
import os
from collections import deque
sys.path.append("../" + os.path.dirname(os.path.realpath(__file__)))
sys.path.append(os.path.dirname(os.path.realpath(__file__)) + '/generated/')
import rumour_pb2
import rumour_pb2_grpc
import time
import machine_info

node_port = 3000
channels = []
whole_mesh_dict = {}
network_meta_dict = {}
heartbeat_meta_dict = {}
suspended_nodes = []
removed_nodes = []
holes = []
mydetails = {"ip": "10.0.0.3", "pos": "(0,1)"}
gossip_queue = deque()
global myheartbeatcount

def sendheartbeat(channel,mydetails,myheartbeatcount):
    rumour_stub = rumour_pb2_grpc.RumourStub(channel)
    rumour_stub.sendheartbeat(rumour_pb2.HeartBeatRequest(ip=mydetails.ip,
                                                    pos=mydetails.pos,
                                                    heartbeatcount=myheartbeatcount,
                                                    wholemesh=str(whole_mesh_dict)
                                                    ))

def receiveheartbeat(channel):
    rumour_stub = rumour_pb2_grpc.RumourStub(channel)
    response = rumour_stub.receiveheartbeat(rumour_pb2.HeartBeatReply())
    if response.pos in whole_mesh_dict:
        if response.ip == whole_mesh_dict[response.pos]:
            heartbeat_meta_dict[response.pos+"-"+response.ip] = response.heartbeatcount
        elif response.ip != whole_mesh_dict[response.pos]:
            heartbeat_meta_dict.remove(response.pos+"-"+whole_mesh_dict[response.pos])
            whole_mesh_dict[response.pos] = response.ip
            heartbeat_meta_dict[response.pos+"-"+response.ip] = response.heartbeatcount
    else:
        whole_mesh_dict[response.pos] = response.ip
        heartbeat_meta_dict[response.pos+"-"+response.ip] = response.heartbeatcount

def checkheartbeats(myheartbeatcount):
    for node in heartbeat_meta_dict:
        if (myheartbeatcount-heartbeat_meta_dict[node]) == 3:
            suspended_nodes.append(node)
        elif (myheartbeatcount-heartbeat_meta_dict[node]) >= 5:
            removed_nodes.append(node)
            whole_mesh_dict.remove(node.split('-')[0])


def sendRumour(channel,victimnodedetails,action,ishole,count):
    rumour_stub = rumour_pb2_grpc.RumourStub(channel)
    if action == "add":
        rumour_stub.sendMyData(rumour_pb2.MyDataRequest(victim_ip=victimnodedetails.ip,
                                                    victim_pos=victimnodedetails.pos,
                                                    action="add",
                                                    ishole="-",
                                                    wholemesh=whole_mesh_dict,
                                                    count=count
                                                    ))
    elif action == "delete":
        rumour_stub.sendMyData(rumour_pb2.MyDataRequest(victim_ip=victimnodedetails.ip,
                                                    victim_pos=victimnodedetails.pos,
                                                    action="delete",
                                                    ishole=ishole,
                                                    wholemesh=whole_mesh_dict,
                                                    count=count
                                                    ))

def receiveRumour(channel):
    rumour_stub = rumour_pb2_grpc.RumourStub(channel)
    response = rumour_stub.receiveRumourData(rumour_pb2.RumourDataRequest())
    hashed_value = hash(response.victim_ip,response.victim_pos)
    action = response.action
    if action == "add":
        if hashed_value not in network_meta_dict:
            network_meta_dict[hashed_value] = (response.victim_ip,response.victim_pos)
            whole_mesh_dict[response.victim_pos] = response.victim_ip
        if response.count+1 < 10:
            for channel in channels:
                sendRumour(channel, {"ip": response.victim_ip, "pos": response.victim_pos}, "add", "-", response.count+1)
    elif action == "delete":
        if hashed_value in network_meta_dict:
            if response.ishole == "true":
                holes.append(response.victim_pos)
                network_meta_dict.remove(hashed_value)
                whole_mesh_dict.remove(response.victim_pos)
                if response.count + 1 < 10:
                    for channel in channels:
                        sendRumour(channel, {"ip": response.victim_ip, "pos": response.victim_pos}, "delete", "true", response.count + 1)
            elif response.ishole == "false":
                network_meta_dict.remove(hashed_value)
                whole_mesh_dict.remove(response.victim_pos)
                if response.count + 1 < 10:
                    for channel in channels:
                        sendRumour(channel, {"ip": response.victim_ip, "pos": response.victim_pos}, "delete", "false", response.count + 1)
        else:
            holes.append(response.victim_pos)

def sendMemoryDetails(channel):
    rumour_stub = rumour_pb2_grpc.RumourStub(channel)
    response = rumour_stub.receiveMemoryRequest(rumour_pb2.MemoryDataRequest())
    if response.request == "send":
        rumour_stub.sendMemoryData(rumour_pb2.MemoryDataRequest(cpu_usage=machine_info.get_my_cpu_usage(),
                                                                 memory_usage=machine_info.get_my_memory_usage(),
                                                                 disk_usage=machine_info.get_my_disk_usage))

def receiveMemoryDetails():
    while True:
        for channel in channels:
            sendMemoryDetails(channel)


def gossip():
    while True:
        time.sleep(5)
        while gossip_queue:
            element = gossip_queue.popleft()
            for channel in channels:
                sendRumour(channel, {"ip": element.ip,"pos": element.pos}, "add", "-", 1)
        for channel in channels:
            receiveRumour(channel)

def heartbeat():
    while True:
        myheadbeatcount = myheadbeatcount + 1
        for channel in channels:
            sendheartbeat(channel, mydetails, myheadbeatcount)
            receiveheartbeat(channel)
        checkheartbeats()











