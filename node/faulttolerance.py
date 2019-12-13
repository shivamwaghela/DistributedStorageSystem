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
import random
node_port = 3000
channels = []

mydetails = {"ip": "10.0.0.3", "pos": "(0,1)"}

def checkforholes():
    global whole_mesh_dict, holes, removed_nodes, lonelyedgequeue
    for node in removed_nodes:
        node = node[1,-1].split(',')
        x = eval(node[0])
        y = eval(node[1])
        top = "("+str(x-1)+","+str(y)+")"
        bottom = "("+str(x+1)+","+str(y)+")"
        left = "("+str(x)+","+str(y-1)+")"
        right = "("+str(x)+","+str(y+1)+")"
        dirs = [top,bottom,left,right]
        ishole = 1
        for dir in dirs:
            if dir not in whole_mesh_dict:
                ishole = 0
        if ishole == 1:
            holes.append(node)
            checkforlonelyedge()
            switchedgenodetohole()

def checkifEdge(node):
    global whole_mesh_dict
    x = node[0]
    y = node[1]
    min_row = sys.maxsize
    min_col = sys.maxsize
    max_row = -sys.maxsize
    max_col = -sys.maxsize
    for element in whole_mesh_dict:
        element = element[1, -1].split(',')
        x = eval(element[0])
        y = eval(element[1])
        min_row = min(min_row, x)
        min_col = min(min_col, y)
        max_row = max(max_row, x)
        max_col = max(max_col, y)
    if x in [min_row,max_row] and y in [max_col,min_col]:
        return True
    return False

def shouldITakeResponsibility(node):
    global whole_mesh_dict,my_pos
    x = node[0]
    y = node[1]
    top = "(" + str(x - 1) + "," + str(y) + ")"
    bottom = "(" + str(x + 1) + "," + str(y) + ")"
    left = "(" + str(x) + "," + str(y - 1) + ")"
    right = "(" + str(x) + "," + str(y + 1) + ")"
    if my_pos != top and top in whole_mesh_dict:
        return False
    elif my_pos != left and left in whole_mesh_dict:
        return False
    elif my_pos != bottom and bottom in whole_mesh_dict:
        return False
    elif my_pos != right and right in whole_mesh_dict:
        return False
    else:
        return True

def sendRequestForEdgeNode(channel,dir1,dir2,deleted_node,neighbours):
    rumour_stub = rumour_pb2_grpc.RumourStub(channel)
    rumour_stub.sendRequestForEdge(rumour_pb2.EdgeNodeRequest(deletednode_pos=deleted_node,
                                                               deletednode_neighbours=neighbours,
                                                               dir1=dir1,
                                                               dir2=dir2
                                                             ))

def receiveRequestForEdgeNode(channel):
    global whole_mesh_dict,my_pos
    rumour_stub = rumour_pb2_grpc.RumourStub(channel)
    response = rumour_stub.receiveRequestForEdge(rumour_pb2.EdgeNodeReply())
    dirs = {"top": [-1, 0],
    "bottom": [1, 0],
    "left": [0, -1],
    "right": [0, 1]
    }
    neighbour1 = dirs[response.dir1]
    neighbour2 = dirs[response.dir2]
    my_pos_update = my_pos[1, -1].split(',')
    neighbour_node1 = "("+eval(neighbour1[0] + my_pos_update[0])+","+eval(neighbour1[1] + my_pos_update[1])+")"
    neighbour_node2 = "("+eval(neighbour2[0] + my_pos_update[0])+","+eval(neighbour2[1] + my_pos_update[1])+")"
    if neighbour_node2 in whole_mesh_dict:
        



def switchedgenodetohole(node):
    global whole_mesh_dict, lonelyedgequeue
    if shouldITakeResponsibility(node) == True:
        x = node[0]
        y = node[1]
        choice1 = random.choice(["top", "bottom"])
        choice2 = random.choice(["left", "right"])
        deleted_node_neighbours = {}
        top = "(" + str(x - 1) + "," + str(y) + ")"
        bottom = "(" + str(x + 1) + "," + str(y) + ")"
        left = "(" + str(x) + "," + str(y - 1) + ")"
        right = "(" + str(x) + "," + str(y + 1) + ")"
        dirs = [top,bottom,left,right]
        for dir in dirs:
            if dir in whole_mesh_dict:
                deleted_node_neighbours[dir] = whole_mesh_dict[dir]
        #get channel
        channel = ""
        sendRequestForEdgeNode(channel,choice1,choice2,node, deleted_node_neighbours)

def checkforlonelyedge():
    global whole_mesh_dict, lonelyedgequeue
    for node in whole_mesh_dict:
        node = node[1, -1].split(',')
        x = eval(node[0])
        y = eval(node[1])
        top = "(" + str(x - 1) + "," + str(y) + ")"
        bottom = "(" + str(x + 1) + "," + str(y) + ")"
        left = "(" + str(x) + "," + str(y - 1) + ")"
        right = "(" + str(x) + "," + str(y + 1) + ")"
        dirs = [top, bottom, left, right]
        count = 0
        for dir in dirs:
            if dir in whole_mesh_dict:
                count=count+1
        if count == 3:
            lonelyedgequeue.append(node)

















