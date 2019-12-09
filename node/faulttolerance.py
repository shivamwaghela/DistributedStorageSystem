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
        if dir in dirs:
            if dir not in whole_mesh_dict:
                ishole = 0
        if ishole == 1:
            holes.append(node)
            checkforlonelyedge()
            switchedgenodetohole()

def switchedgenodetohole():
    print("fill this function")

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

















