import os
import sys
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
        logger.info("Traversal.ReceiveData hash_id:{} request_id:{} stack:{} visited:{}"
                    .format(request.hash_id, request.request_id, request.stack, request.visited))
        # Check if the file exits on current node
        if find_data(request.hash_id):
            return traversal_pb2.ReceiveDataResponse(file_bytes=fetch_data(request.hash_id),
                                                     request_id=request.request_id,
                                                     node_ip=globals.my_ip)

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
                                                 request_id=request.request_id,
                                                 node_ip=forwarded_node_ip,
                                                 status=TraversalResponseStatus.FORWARDED)


# XXX
def forward_receive_data_request(node_ip, request):
    logger.info("forward_receive_data_request: node_ip: {}".format(node_ip))
    channel = None
    for item in globals.node_connections.connection_dict.items():
        if item[1].node_ip == node_ip:
            channel = item[1].channel
            break

    traversal_stub = traversal_pb2_grpc.TraversalStub(channel)
    response = traversal_stub.ReceiveData(hash_id=request.hash_id,
                                          request_id=request.request_id,
                                          stack=request.stack,
                                          visited=request.visited)
    logger.info("forward_receive_data_request: response: {}".format(response))
    return response
