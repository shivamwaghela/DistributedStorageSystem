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


logger = logging.getLogger(__name__)
gossip_dictionary = {"10.0.0.1": (0,0), "10.0.0.3": (0,2), "10.0.0.2": (0,1), "10.0.0.5": (1,2), "10.0.0.6": (1,0), "10.0.0.4": (1,1), "10.0.0.7": (2,1), "10.0.0.9": (2,0), "10.0.0.8": (2,2)}
q = PriorityQueue()

# XXX
def find_data(hash_id):
    return random.choice([True, False])


# XXX
def fetch_data(hash_id):
    return "data"


# XXX
class Traversal(traversal_pb2_grpc.TraversalServicer):
    def ReceiveData(self, request, context):
        TraversalResponseStatus = traversal_response_status.TraversalResponseStatus
        logger.info("Traversal.ReceiveData hash_id:{} request_id:{} visited:{}"
                    .format(request.hash_id, request.request_id, request.visited))
        print("Traversal.ReceiveData hash_id:{} request_id:{} visited:{}"
                    .format(request.hash_id, request.request_id, request.visited))
        data_found = True
        # Check if the file exits on current node
        if data_found:
            curr_data = fetch_data(request.hash_id)
            curr_mesh = self.create_logical_mesh()
            curr_path = self.find_shortest_path(curr_mesh)
            self.forward_response_data(curr_data, request.request_id, request.node_ip, traversal_response_status.FOUND,
                                       curr_path)
            # RespondData(file_bytes=curr_data, request_id=request.request_id, node_ip = request.node_ip, status = traversal_response_status.FOUND, path = curr_path)
            return traversal_pb2.ReceiveDataResponse(status = str(traversal_response_status.FOUND))

        # If file not found in node
        # add neighbors to stack. before adding check if neighbor is already visited.
        visited = eval(request.visited)
        
        if globals.my_ip not in visited:
            visited.append(globals.my_ip)

        logger.info("Traversal.ReceiveData: visited: {}".format(visited))
        print(visited)

        neighbor_list = []

        for item in globals.node_connections.connection_dict.items():
            if True: #item.channel.isAlive(): #confirm
                print("Node IP: {}".format(item[1].node_ip))
                print("Traversal.ReceiveData hash_id:{} request_id:{} visited:{}"
                    .format(request.hash_id, request.request_id, request.visited))
                neighbor_list.append(item)

        forward_list = []

        for item in neighbor_list:
            if item not in visited:
                visited.append(item)
                forward_list.append(item)

        print("Forwarded List: {}".format(forward_list))
        print("Neighbor List: {}".format(neighbor_list))
        print("Visited List: {}".format(visited))
        threading_list = []
        for item in forward_list:
            forwarded_node_ip = item.node_ip #confirm
            channel = item.channel #confirm
            print("Forwarded Node IP: {}".format(forwarded_node_ip))
            print("Channel: {}".format(channel))
            forward_request_thread = threading.Thread(target=self.forward_receive_data_request, args=(forwarded_node_ip, channel, request))
            threading_list.append(forward_request_thread)

        for thread in threading_list:    
            thread.start()
            thread.join()

        return traversal_pb2.ReceiveDataResponse(status=str(TraversalResponseStatus.FORWARDED)) # confirm indentation

    def RespondData(self, request, context):
        t = threading.Thread(target=self.forward_response_data, args=(request.file_bytes, request.request_id, request.node_ip, request.status, request.path))
        t.start()
        return traversal_pb2.ResponseDataResponse(status = str(traversal_response_status.FORWARDED))



    #XXX
    def forward_receive_data_request(self, node_ip, channel, request):
        logger.info("forward_receive_data_request: node_ip: {}".format(node_ip))

        traversal_stub = traversal_pb2_grpc.TraversalStub(channel)
        response = traversal_stub.ReceiveData(
            traversal_pb2.ReceiveDataRequest(
                                hash_id=str(request.hash_id),
                                request_id=str(request.request_id),
                                stack=str(request.stack),
                                visited=str(request.visited)))
        logger.info("forward_receive_data_request: response: {}".format(response))
        return response

    def forward_response_data(self, file_bytes, request_id, node_ip, status, path):
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
        logger.info("Current response is: response : {}".format(response))
        return response


    #creating a 2D matrix to keep track of live and dead nodes
    def create_logical_mesh(self):
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
        logger.info("Current mesh is: mesh : {}".format(mesh))
        return mesh

    def find_shortest_path(self, mesh):
        path = ""
        return path