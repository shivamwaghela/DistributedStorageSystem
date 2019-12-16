import sys
import os
import grpc
import numpy as np
import collections

sys.path.append("../" + os.path.dirname(os.path.realpath(__file__)))
sys.path.append(os.path.dirname(os.path.realpath(__file__)) + '/utils/')
sys.path.append(os.path.dirname(os.path.realpath(__file__)) + '/generated/')

import replication_pb2
import replication_pb2_grpc
import globals

def replication(path_one, message_stream_of_chunk_bytes, metadata):
    serverAddress = globals.my_ip
    serverPort = 50051
    channel = grpc.insecure_channel(serverAddress + ":" + str(serverPort))
    replicate_stub = replication_pb2_grpc.FileserviceStub(channel)
    metadata = (
        ('key-hash-id', metadata["key-hash-id"]),
        ('key-chunk-size', str(metadata["key-chunk-size"])),
        ('key-number-of-chunks', str(metadata["key-number-of-chunks"])),
        ('key-is-replica', metadata["key-is-replica"])
    )
    # fileMeta = replication_pb2.FileMetaData(key_hash_id = metadata.key_hash_id,
    #                                         key_chunk_size = metadata.key_chunk_size,
    #                                         key_number_of_chunks = metadata.key_number_of_chunks,
    #                                         key_is_replica = metadata.key_is_replica)
    request = replication_pb2.FileData(initialReplicaServer=globals.my_ip, bytearray=message_stream_of_chunk_bytes,
                                       vClock="My V Clock", shortest_path=path_one, currentpos=0)
    resp = replicate_stub.ReplicateFile(request, metadata = metadata)
    return resp

def get_best_path(whole_mesh_dict, destination_ipaddress):
    original_list = list(whole_mesh_dict.keys())
    my_list = original_list
    max_rows = -sys.maxsize - 1
    for item in range(len(my_list)):
        if my_list[item][0] > max_rows:
            max_rows = my_list[item][0]

    max_cols = -sys.maxsize - 1
    for item in range(len(my_list)):
        if my_list[item][1] > max_cols:
            max_cols = my_list[item][1]

    max_rows += 1
    max_cols += 1

    for x in range(max_rows):
        for y in range(max_cols):
            if (x, y) not in my_list:
                my_list.append((x, y))

    destination_cooridinates = None
    for key, value in whole_mesh_dict.items():
        if destination_ipaddress == value:
            destination_cooridinates = key
            break

    source_cooridinates = globals.my_coordinates
    print(my_list)
    my_list = sorted(my_list, key=lambda k: [k[1], k[0]])
    my_list = sorted(my_list, key=lambda k: [k[0], k[1]])
    dicty = {}
    counter = 0
    listy = []

    for i in my_list:
        dicty[counter] = i
        listy.append(counter)
        counter += 1

    x = np.array(listy)
    a = np.reshape(x, (max_rows, max_cols))
    string_list = []

    for i in range(len(a)):
        temp = ""
        for j in range(len(a[i])):
            if (i, j) in original_list and (i, j) == destination_cooridinates:
                temp += "$"
            elif (i, j) in original_list:
                temp += "."
            else:
                temp += "#"
        string_list.append(temp)
    goal = "$"

    def bfs(grid, start):
        queue = collections.deque([[start]])
        seen = set([start])
        while queue:
            path = queue.popleft()
            x, y = path[-1]
            if grid[y][x] == goal:
                return path
            for x2, y2 in ((x + 1, y), (x - 1, y), (x, y + 1), (x, y - 1)):
                if 0 <= x2 < max_cols and 0 <= y2 < max_rows and grid[y2][x2] != "$" and (x2, y2) not in seen:
                    queue.append(path + [(x2, y2)])
                    seen.add((x2, y2))

    path = bfs(string_list, (source_cooridinates[1], source_cooridinates[0]))
    print("PATH==", path)
    return path


