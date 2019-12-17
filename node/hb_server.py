# import grpc
# from concurrent import futures
# import os
# import sys
# import globals

# sys.path.append("../" + os.path.dirname(os.path.realpath(__file__)))
# sys.path.append(os.path.dirname(os.path.realpath(__file__)) + '/generated/')
# sys.path.append(os.path.dirname(os.path.realpath(__file__)) + '/utils/')

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

import rumour_pb2
import rumour_pb2_grpc


whole_mesh_dict = {'(0,0)': '10.0.0.1'}
heartbeat_meta_dict = {'127.0.0.1': 1}

"""
Failure detected -> api invoked
api to inform middleware about the holes in the mesh
Converse : 
    Megha : tell her about the api parameters which she needs to send
    Wamik : ask him about ip,port,stubs and protos
"""

#ip and port of middleware server - Needs to be changed
import recovery_pb2
import recovery_pb2_grpc
middleware_ip = "10.0.0.30"
middleware_port = "8888"

def sendHoleInfoToMiddleware(node_pos,node_neighbors):
    print("Inside hole info send")

    #grpc channel with middleware
    middleware_channel = grpc.insecure_channel(middleware_ip + ":" + str(middleware_port))
    midddleware_stub = recovery_pb2_grpc.RecoveryStub(middleware_channel)
    #closing a channl ? - Megha
    response = midddleware_stub.sendHoleInfo(recovery_pb2.SendHoleInfoRequest(pos = str(node_pos),neighbors = str(node_neighbors)))


def updatehearbeatdict(newnodeheartbeatdict):
    for node in newnodeheartbeatdict:
        if node in heartbeat_meta_dict:
            heartbeat_meta_dict[node] = max(heartbeat_meta_dict[node], newnodeheartbeatdict[node])
        else:
            heartbeat_meta_dict[node] = newnodeheartbeatdict[node]

def updatemeshdict(newnodemeshdict):
    print("new node mesh....")
    print(newnodemeshdict)
    for node in newnodemeshdict:
        if node not in whole_mesh_dict:
            whole_mesh_dict[node] = newnodemeshdict[node]
        else:
            if whole_mesh_dict[node] != newnodemeshdict[node]:
                whole_mesh_dict[node] = newnodemeshdict[node]


class RumourServicer(rumour_pb2_grpc.RumourServicer):

    # client -> server   server.rpc
    def sendheartbeat (self, request, context):
        print("in receive", request)
        response = request #fetch request here
        if  response.pos in whole_mesh_dict and response.ip != whole_mesh_dict[response.pos]:
            heartbeat_meta_dict.remove(response.pos+"-"+whole_mesh_dict[response.pos])
            whole_mesh_dict[response.pos] = response.ip
            heartbeat_meta_dict[response.pos+"-"+response.ip] = response.heartbeatcount

        newnodemeshdict = eval(response.wholemesh)
        newnodeheartbeatdict = eval(response.heartbeatdict)
        updatehearbeatdict(newnodeheartbeatdict)
        if newnodemeshdict:
            updatemeshdict(newnodemeshdict)

        #check removed nodes
        removednodes = []
        my_ip = globals.my_ip
        src_removed_node_dict = eval(request.removed_node_dict)
        for key in src_removed_node_dict:
            my_hb = heartbeat_meta_dict[key] if key in heartbeat_meta_dict else 0
            if (heartbeat_meta_dict[my_ip] - my_hb >= 5):
                removednodes.append(key)

        return rumour_pb2.HeartBeatReply(removednodes=str(removednodes))
        

import globals
import connection
import grpc
from node_position import NodePosition
import network_manager_pb2
import network_manager_pb2_grpc
class RecoveryServicer(recovery_pb2_grpc.RecoveryServicer):
    """
    this should come in the shivam's code
    Recovery RPC
    node -> recovery -> current channels closed -> neig ips , new channels - shivams
    rpc  reposition(message)
    message : {
        pos :
        neigh : {}
    }
    """
    def __init__(self):
        #telling M/W about adding new node
        middleware_channel = grpc.insecure_channel(middleware_ip + ":" + str(middleware_port))
        midddleware_stub = recovery_pb2_grpc.RecoveryStub(middleware_channel)
        midddleware_stub.sendAdditionOfNodeMessage(recovery_pb2.AdditionalOfNodeRequest(pos = str(globals.my_coordinates),ip = str(globals.my_ip)))


    def recovery(self,request, context):
        print("Recovery initiated by Middleware Server", request)

        #update the pos of the node
        globals.my_coordinates = request.pos

        #delete the curr neighs & channels
        for item in list(globals.node_connections.connection_dict.items()):
            if item[0] == NodePosition.CENTER:
                    continue
            try:
                globals.node_connections.remove_connection(item[0])
            except:
                continue


        #create new channels btw curr node and neighbor
        try:
            node_neighbors = eval(request.neighbors)
            for key in node_neighbors :
                elem  = eval(node_neighbors[key])
                neighbor_ip = elem.ip
                neighbor_pos = elem.pos
                neighbor_channel = grpc.insecure_channel(neighbor_ip + ":" + str(globals.port))

                #creating channel froom the neighbor to curr node
                network_manager_stub = network_manager_pb2_grpc.NetworkManagerStub(neighbor_channel)
                response = network_manager_stub.UpdateNeighborMetaData(network_manager_pb2.UpdateNeighborMetaDataRequest(client_node_ip=globals.my_ip,
                                                                  client_node_coordinates=str(globals.my_coordinates)))
                
                #creating channel from curr node to  neighbor
                neighbor_node_conn = connection.Connection(channel=neighbor_channel,
                                                     node_position=NodePosition[key.upper()].value,
                                                     node_coordinates=neighbor_pos,
                                                     node_ip=neighbor_ip)
                globals.node_connections.add_connection(neighbor_node_conn)
        except:
            print("Exception in Recovery")
            pass

        return recovery_pb2.StartRecoveryReply()


    def sendWholeMesh(self,request, context):
        print("Inside sendWholeMesh",whole_mesh_dict)
        return recovery_pb2.SendWholeMeshReply(wholemesh = str(whole_mesh_dict))

if __name__ == "__main__":
    # print(str(sys.argv))
    # argv = sys.argv
    # my_ip = argv[1]
    # heartbeat_meta_dict[my_ip] = 1
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    rumour_pb2_grpc.add_RumourServicer_to_server(RumourServicer(), server)

    # recovery server
    recovery_pb2_grpc.add_RecoveryServicer_to_server(RecoveryServicer(), server)

    server.add_insecure_port('[::]:8888')
    server.start()
    print("Server starting...")

    server.wait_for_termination()