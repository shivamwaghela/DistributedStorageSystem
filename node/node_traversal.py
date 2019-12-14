import os
import sys
from queue import PriorityQueue
sys.path.append("../" + os.path.dirname(os.path.realpath(__file__)))
sys.path.append(os.path.dirname(os.path.realpath(__file__)) + '/utils/')
sys.path.append(os.path.dirname(os.path.realpath(__file__)) + '/generated/')

import globals
import logging
import random
import traversal_response_status
import traversal_pb2
import traversal_pb2_grpc
import threading
from queue import *

logger = logging.getLogger(__name__)
gossip_dictionary = {"10.0.0.1": (0,0), "10.0.0.3": (0,2), "10.0.0.2": (0,1), "10.0.0.5": (1,2), "10.0.0.6": (1,0), "10.0.0.4": (1,1), "10.0.0.7": (2,1), "10.0.0.9": (2,0), "10.0.0.8": (2,2)}
q = PriorityQueue()

# up, left, right, down movements
row = [-1, 0, 0, 1]
col = [0, -1, 1, 0]

# source coordinates (1,0)
source_x = 0
source_y = 1

# destination coordinates (9,10)
destination_x = 3
destination_y = 5

# XXX
def find_data(hash_id):
    return random.choice([True, False])


# XXX
def fetch_data(hash_id):
    return "data"

# Nodes for shortest path
class Node:
    def __init__(self, x, y, w, parent):
        self.x = x
        self.y = y
        self.dist = w
        self.parent = parent

# XXX
class Traversal(traversal_pb2_grpc.TraversalServicer):
    def ReceiveData(self, request, context):
        TraversalResponseStatus = traversal_response_status.TraversalResponseStatus
        logger.info("Traversal.ReceiveData hash_id:{} request_id:{} stack:{} visited:{}"
                    .format(request.hash_id, request.request_id, request.stack, request.visited))
        # Check if the file exits on current node
        if True:
            curr_data = fetch_data(request.hash_id)
            curr_mesh = create_logical_mesh()
            curr_path = find_shortest_path(curr_mesh)
            forward_response_data(curr_data, request.request_id, request.node_ip, traversal_response_status.FOUND, curr_path)
            # RespondData(file_bytes=curr_data, request_id=request.request_id, node_ip = request.node_ip, status = traversal_response_status.FOUND, path = curr_path)
            return traversal_pb2.ReceiveDataResponse(status = traversal_response_status.FOUND)

        # add neighbors to stack. before adding check if neighbor is already visited.
        stack = eval(request.stack)
        visited = eval(request.visited)

        if len(stack) == 0 and len(visited) != 0:
            logger.info("Traversal.ReceiveData: len(stack) == 0 and len(visited) != 0")
            # visited all the nodes but file not found
            return traversal_pb2.ReceiveDataResponse(file_bytes=None,
                                                     request_id=request.request_id,
                                                     node_ip=globals.my_ip,
                                                     status=TraversalResponseStatus.NOT_FOUND)

        # forward the request
        visited.append(globals.my_ip)
        logger.info("Traversal.ReceiveData: visited: {}".format(visited))

        # add neighbors to stack
        for item in globals.node_connections.connection_dict.items():
            if item[1].node_ip not in visited:
                stack.append(item[1].node_ip) # items = {TOP: conn obj}

        logger.info("Traversal.ReceiveData: stack: {}".format(stack))
        forwarded_node_ip = ""
        while len(stack) > 0:
            forwarded_node_ip = stack.pop()
            if forwarded_node_ip not in visited:
                break

        logger.info("Traversal.ReceiveData: forwarded_node_ip: {}".format(forwarded_node_ip))

        forward_request_thread = threading.Thread(target=forward_receive_data_request, args=(forwarded_node_ip, request))
        forward_request_thread.start()

        return traversal_pb2.ReceiveDataResponse(file_bytes=None,
                                                 request_id=str(request.request_id),
                                                 node_ip=str(forwarded_node_ip),
                                                 status=str(TraversalResponseStatus.FORWARDED))

    def RespondData(self, request, context):
            t = threading.Thread(target=forward_response_data, args=(request.file_bytes, request.request_id, request.node_ip, request.status, request.path))
            t.start()
            return traversal_pb2.ResponseDataResponse(status = traversal_response_status.FORWARDED)



# XXX
def forward_receive_data_request(node_ip, request):
    logger.info("forward_receive_data_request: node_ip: {}".format(node_ip))
    channel = None
    for item in globals.node_connections.connection_dict.items():
        if item[1].node_ip == node_ip:
            channel = item[1].channel
            break

    traversal_stub = traversal_pb2_grpc.TraversalStub(channel)
    response = traversal_stub.ReceiveData(
        traversal_pb2.ReceiveDataRequest(
                            hash_id=str(request.hash_id),
                            request_id=str(request.request_id),
                            stack=str(request.stack),
                            visited=str(request.visited)))
    logger.info("forward_receive_data_request: response: {}".format(response))
    return response

def forward_response_data(file_bytes, request_id, node_ip, status, path):
    curr_path = eval(path)
    curr_coordinates = curr_path.pop()
    
    #check if data reached the initial invoking node
    if curr_path.empty():
        return file_bytes
    
    #get the channel through which the data will be propogated
    for item in globals.node_connections.connection_dict.items():
        if item[1].node_coordinates == curr_coordinates:
            channel = item[1].channel
            break
    
    #check if channel is alive. if not, update the 2D matrix and calculate a new shortest path
    if not channel.isAlive():
        mesh = create_logical_mesh()
        mesh[curr_coordinates[0]][curr_coordinates[1]] = 0
        curr_path = find_shortest_path(mesh)
        forward_response_data(file_bytes, request_id, node_ip, status, curr_path)

    #forward the request
    traversal_stub = traversal_pb2_grpc.TraversalStub(channel)
    response = traversal_stub.RespondData(
        traversal_pb2.RespondDataRequest(
            file_bytes=file_bytes,
            request_id=request_id,
            node_ip=node_ip,
            status=status,
            path=curr_path
        ))
    return response


#creating a 2D matrix to keep track of live and dead nodes
def create_logical_mesh():
    min_row = list(gossip_dictionary.values())[0][0]
    min_col = list(gossip_dictionary.values())[0][1]
    max_row = list(gossip_dictionary.values())[len(gossip_dictionary)-1][0]
    max_col = list(gossip_dictionary.values())[len(gossip_dictionary)-1][1]

    for key in gossip_dictionary:
        if gossip_dictionary[key][0] < min_row:
            min_row = gossip_dictionary[key][0]
        if gossip_dictionary[key][1] < min_col:
            min_col = gossip_dictionary[key][1]
        if gossip_dictionary[key][0] > max_row:
            max_row = gossip_dictionary[key][0]
        if gossip_dictionary[key][1] > max_col:
            max_col = gossip_dictionary[key][1]

    cols = max_col - min_col + 1
    rows = max_row - min_col + 1

    mesh = [[0]*cols]*rows
    
    value_list = list(gossip_dictionary.values())
    for item in value_list:
        mesh[item[0]][item[1]] = 1
    
    return mesh

def find_shortest_path(mesh):
    path = []
    # M is total number of rows in matrix, N is total number of columns in matrix
    M = len(mesh)
    N = len(mesh[0])
    node = shortestDistance(mesh)

    if node != None:
        print("The shortest safe path has length of ", node.dist, "\nCoordinates of the path are :\n")
        path = printPath(node)
    else:
        print("No route is safe to reach destination")
    return path

def isSafe(field, visited, x, y):
    return field[x][y] == 1 and not visited[x][y]

def isValid(x, y, mesh):
    M = len(mesh)
    N = len(mesh[0])
    return x < M and y < N and x >= 0 and y >= 0

def BFS(mesh):
    # created a visited list of Dimensions M*N that stores if a cell is visited or not
    visited = []
    M = len(mesh)
    N = len(mesh[0])
    m = 0
    while m < M:
        visited_row = [False] * N
        visited.append(visited_row)
        m += 1

    # create an empty queue
    q = Queue()
    # put source coodinates in the queue

    q.put(Node(source_x, source_y, 0, None))
    # run till queue is not empty
    while q.qsize() > 0:
        node = q.get()
        # pop front node from queue and process it
        i = node.x
        j = node.y
        dist = node.dist
        # if destination is found, return minimum distance
        if i == destination_x and j == destination_y:
            return node
        # check for all 4 possible movements from current cell and enqueue each valid movement
        k = 0
        while k < 4:
            # Skip if location is invalid or visited or unsafe
            # print("in BFS" , i + row[k], j + col[k])
            if isValid(i + row[k], j + col[k], mesh) and isSafe(mesh, visited, i + row[k], j + col[k]):
                # mark it visited & push it into queue with +1 distance
                visited[i + row[k]][j + col[k]] = True
                q.put(Node(i + row[k], j + col[k], dist + 1, node))
            k += 1
    return None


# Find Shortest Path from first column to last column in given field
def shortestDistance(mesh):
    # update the mesh
    i = 0
    M = len(mesh)
    N = len(mesh[0])
    while i < M:
        j = 0
        while j < N:
            if mesh[i][j] == float('inf'):
                mesh[i][j] = 0
            j += 1
        i += 1

    # call BFS and return shortest distance found by it
    return BFS(mesh)


def printPath(node):
    if not node:
        return
    printPath(node.parent)
    co_ords = (node.x, node.y)
    path = []
    path.append(co_ords)
    return path
   # print("{", node.x, node.y, "}")