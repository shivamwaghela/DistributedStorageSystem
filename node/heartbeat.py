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

mydetails = {"ip": "10.0.0.3", "pos": "(0,1)"}

def sendheartbeat(channel,mydetails,myheartbeatcount):
    global heartbeat_meta_dict
    rumour_stub = rumour_pb2_grpc.RumourStub(channel)
    rumour_stub.sendheartbeat(rumour_pb2.HeartBeatRequest(ip=mydetails.ip,
                                                    pos=mydetails.pos,
                                                    heartbeatcount=myheartbeatcount,
                                                    wholemesh=str(whole_mesh_dict),
                                                    heartbeatdict=str(heartbeat_meta_dict),
                                                    ))

def updatehearbeatdict(newnodeheartbeatdict):
    for node in newnodeheartbeatdict:
        if node in heartbeat_meta_dict:
            heartbeat_meta_dict[node] = max(heartbeat_meta_dict[node], newnodeheartbeatdict[node])
        else:
            heartbeat_meta_dict[node] = newnodeheartbeatdict[node]

def updatemeshdict(newnodemeshdict):
    for node in newnodemeshdict:
        if node not in whole_mesh_dict:
            whole_mesh_dict[node] = newnodemeshdict[node]
        else:
            if whole_mesh_dict[node] != newnodemeshdict[node]:
                whole_mesh_dict[node] = newnodemeshdict[node]

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
    newnodemeshdict = eval(response.wholemesh)
    newnodeheartbeatdict = eval(response.heartbeatdict)
    updatehearbeatdict(newnodeheartbeatdict)
    updatemeshdict(newnodemeshdict)

def checkheartbeats(myheartbeatcount):
    for node in heartbeat_meta_dict:
        if (myheartbeatcount-heartbeat_meta_dict[node]) == 3:
            suspended_nodes.append(node)
        elif (myheartbeatcount-heartbeat_meta_dict[node]) >= 5:
            removed_nodes.append(node)
            whole_mesh_dict.remove(node.split('-')[0])
            heartbeat_meta_dict.remove(node.split('-')[0])
        else:
            if node in suspended_nodes:
                suspended_nodes.remove(node)

def heartbeat():
    global whole_mesh_dict, heartbeat_meta_dict, suspended_nodes, removed_nodes, myheartbeatcount
    while True:
        myheadbeatcount = myheadbeatcount + 1
        for channel in channels:
            sendheartbeat(channel, mydetails, myheadbeatcount)
            receiveheartbeat(channel)
        checkheartbeats()











