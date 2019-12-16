import grpc
import os
import sys
from collections import deque
import time
import hb_server

sys.path.append("../" + os.path.dirname(os.path.realpath(__file__)))
sys.path.append(os.path.dirname(os.path.realpath(__file__)) + '/generated/')
sys.path.append(os.path.dirname(os.path.realpath(__file__)) + '/utils/')
import globals
import rumour_pb2
import rumour_pb2_grpc

my_ip = globals.my_ip
my_pos = globals.my_position
channels = []
gossip_queue = deque()
neighbour_dict = []
suspended_nodes = []
global removed_nodes
removed_nodes = []
response_removed_nodes = {}

def sendMsg(server_ip, action, whole_mesh_dict, heartbeat_meta_dict):
    channel = grpc.insecure_channel(server_ip + ':50051')
    rumour_stub = rumour_pb2_grpc.RumourStub(channel)
    mesh_dict = {}
    removed_node_dict = []
    if action:
        mesh_dict = whole_mesh_dict

    if len(removed_nodes) != 0:
        removed_node_dict = removed_nodes

    response = rumour_stub.sendheartbeat(rumour_pb2.HeartBeatRequest(ip=my_ip, pos=my_pos, heartbeatcount=myheartbeatcount, wholemesh=str(mesh_dict),
                                                    heartbeatdict=str(heartbeat_meta_dict), removednodes=str(removed_node_dict)))
    ngbrremovednodes = eval(response.removednodes)
    print("nggg...")
    print(ngbrremovednodes)
    for node in ngbrremovednodes:
        if node in response_removed_nodes:
            response_removed_nodes[node] += 1
        else:
            response_removed_nodes[node] = 1

def markNodes(heartbeat_meta_dict):
    print("in mark nodes.....")
    for node in heartbeat_meta_dict:
        print("in hbbb,,,,,")
        print(myheartbeatcount)
        print(".....")
        print(heartbeat_meta_dict[node])
        # if (myheartbeatcount-heartbeat_meta_dict[node]) >= 3:
        #     suspended_nodes.append(node)
        
        if (myheartbeatcount-heartbeat_meta_dict[node]) >= 3:
            removed_nodes.append(node)

    print("....mk...")
    print(removed_nodes)
            #whole_mesh_dict.remove(node.split('-')[0])


def hb_client():
    global myheartbeatcount, response_removed_nodes 
    myheartbeatcount = 1
    whole_mesh_dict = hb_server.whole_mesh_dict
    heartbeat_meta_dict = hb_server.heartbeat_meta_dict
    
    while True:
        local_mesh = {}
        for item in globals.node_connections.connection_dict.items():
            if item[1].node_coordinates not in whole_mesh_dict:
                gossip_queue.append({"ip":item[1].node_ip,"pos":item[1].node_coordinates})
            if item[1].node_ip != globals.my_ip and item[1].node_ip not in neighbour_dict:
                neighbour_dict.append(item[1].node_ip)
        print(neighbour_dict)
        time.sleep(5) 
        while gossip_queue:
            element = gossip_queue.popleft()
            whole_mesh_dict[element["pos"]] = element["ip"]
            action = "add"

        myheartbeatcount = myheartbeatcount + 1
        print("my ip" + globals.my_ip)
        heartbeat_meta_dict[globals.my_ip] = myheartbeatcount
        markNodes(heartbeat_meta_dict)

        for neighbour in neighbour_dict:
            sendMsg(neighbour, action,whole_mesh_dict,heartbeat_meta_dict)

        if response_removed_nodes:
            for key in response_removed_nodes:
                print("response_rrrm..?C>w. ")
                print(key)
                print(response_removed_nodes[key])
                rell = []
                if response_removed_nodes[key] == len(neighbour_dict)-1:
                    print("Failed node...." + key)
                    for i in whole_mesh_dict:
                        if whole_mesh_dict[i] == key:
                            print("rremoving key......")
                            print(i)
                            rell.append(i)
                            print(whole_mesh_dict[i])

                    for i in rell:
                        print("delerting.........")
                        print(i)
                        del hb_server.whole_mesh_dict[i]

                    rell = []
                    # update logical mesh
                    # inform middleware
                    # remove channels
                    # remove node from heartbeatdict - initiate gossip !!
        
        global removed_nodes
        removed_nodes = []
        response_removed_nodes = {}
             
    print("Client started...")