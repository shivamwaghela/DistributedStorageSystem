import os
import sys

sys.path.append("../" + os.path.dirname(os.path.realpath(__file__)))
sys.path.append(os.path.dirname(os.path.realpath(__file__)) + '/utils/')
sys.path.append(os.path.dirname(os.path.realpath(__file__)) + '/generated/')

import grpc
import time
import storage_pb2, storage_pb2_grpc
from memory_manager import MemoryManager

class StorageManagerServer(storage_pb2_grpc.FileServerServicer):

    def __init__(self, memory_node_bytes, page_memory_size_bytes):
        self.memory_manager = MemoryManager(memory_node_bytes, page_memory_size_bytes)

    def upload_chunk_stream(self, request_iterator, context):
        hash_id = ""
        chunk_size = 0
        number_of_chunks = 0

        for key, value in context.invocation_metadata():
            if key == "key-hash-id":
                hash_id = value
            elif key == "key-chunk-size":
                chunk_size = int(value)
            elif key == "key-number-of-chunks":
                number_of_chunks = int(value)

        assert hash_id != ""
        assert chunk_size != 0
        assert number_of_chunks != 0

        success = self.memory_manager.put_data(request_iterator, hash_id, number_of_chunks, False)
        return storage_pb2.ResponseBoolean(success=success)

    def upload_single_chunk(self, request_chunk, context):
        hash_id = ""
        chunk_size = 0

        for key, value in context.invocation_metadata():
            if key == "key-hash-id":
                hash_id = value
            elif key == "key-chunk-size":
                chunk_size = int(value)

        assert hash_id != ""
        assert chunk_size != 0

        success = self.memory_manager.put_data(request_chunk, hash_id, 1, True)
        return storage_pb2.ResponseBoolean(success=success)

    def download_chunk_stream(self, request, context):
        chunks = self.memory_manager.get_data(request.hash_id)
        for c in chunks:
            yield storage_pb2.ChunkRequest(chunk=c)

    def get_node_available_memory_bytes(self, request, context):
        bytes_ = self.memory_manager.get_available_memory_bytes()
        return storage_pb2.ResponseDouble(bytes=bytes_)

    def get_stored_hashes_list_iterator(self, request, context):
        list_of_hashes = self.memory_manager.get_stored_hashes_list()
        for hash_ in list_of_hashes:
            yield storage_pb2.ResponseString(hash_id=hash_)

    def is_hash_id_in_memory(self, request, context):
        hash_exists = self.memory_manager.hash_id_exists(request.hash_id)
        return storage_pb2.ResponseBoolean(success=hash_exists)

