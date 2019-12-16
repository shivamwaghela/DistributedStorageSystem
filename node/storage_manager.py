import os
import sys
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

sys.path.append("../" + os.path.dirname(os.path.realpath(__file__)))
sys.path.append(os.path.dirname(os.path.realpath(__file__)) + '/utils/')
sys.path.append(os.path.dirname(os.path.realpath(__file__)) + '/generated/')

import storage_pb2, storage_pb2_grpc
from memory_manager import MemoryManager

DEBUG = 0

class StorageManagerServer(storage_pb2_grpc.FileServerServicer):

    def __init__(self, memory_node_bytes, page_memory_size_bytes):
        self.memory_manager = MemoryManager(memory_node_bytes, page_memory_size_bytes)

    def upload_chunk_stream(self, request_iterator, context):
        if DEBUG:
            logger.debug("[storage manager] upload_chunk_stream called")

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

        if DEBUG:
            logger.debug("[storage manager] hash_id: {}".format(hash_id))
            logger.debug("[storage manager] chunk_size: {}".format(chunk_size))
            logger.debug("[storage manager] number_of_chunks: {}".format(number_of_chunks))

        assert hash_id != ""
        assert chunk_size != 0
        assert number_of_chunks != 0


        if DEBUG:
            logger.debug("[storage manager] Assertions passed")

        success = self.memory_manager.put_data(request_iterator, hash_id, chunk_size, number_of_chunks, False)

        if DEBUG:
            logger.debug("[storage manager] called memory manager put data, response:  {}".format(success))

        return storage_pb2.ResponseBoolean(success=success)

    def upload_single_chunk(self, request_chunk, context):
        """
        :param request_chunk: single storage_pb2.ChunkRequest
        :param context: must include 'key-hash-id' and 'key-chunk-size' in metadata
        :return: boolean as storage_pb2.ResponseBoolean
        """
        hash_id = ""
        chunk_size = 0

        if DEBUG:
            logger.debug("[storage manager] upload_single_chunk called")

        for key, value in context.invocation_metadata():
            if key == "key-hash-id":
                hash_id = value
            elif key == "key-chunk-size":
                chunk_size = int(value)

        if DEBUG:
            logger.debug("[storage manager] hash_id: {}".format(hash_id))
            logger.debug("[storage manager] chunk_size: {}".format(chunk_size))

        assert hash_id != ""
        assert chunk_size != 0

        if DEBUG:
            logger.debug("[storage manager] Assertions passed")

        success = self.memory_manager.put_data(request_chunk, hash_id, chunk_size, 1, True)

        if DEBUG:
            logger.debug("[storage manager] called memory manager put data, response:  {}".format(success))

        return storage_pb2.ResponseBoolean(success=success)

    def download_chunk_stream(self, request, context):
        if DEBUG:
            logger.debug("[storage manager] download chunk stream called")

        chunks = self.memory_manager.get_data(request.hash_id)

        for c in chunks:
            yield storage_pb2.ChunkRequest(chunk=c)

    def get_node_available_memory_bytes(self, request, context):
        """
        :param request: storage_pb2.EmptyRequest()
        :param context: None
        :return: double value as storage_pb2.ResponseDouble
        """

        if DEBUG:
            logger.debug("[storage manager] get node available memory bytes called")

        bytes_ = self.memory_manager.get_available_memory_bytes()
        return storage_pb2.ResponseDouble(bytes=bytes_)

    def get_stored_hashes_list_iterator(self, request, context):
        """
        :param request: storage_pb2.EmptyRequest()
        :param context: None
        :return: stream of storage_pb2.storage_pb2.ResponseString
        """

        if DEBUG:
            logger.debug("[storage manager] get stored_hashes list iterator called")

        list_of_hashes = self.memory_manager.get_stored_hashes_list()
        for hash_ in list_of_hashes:
            yield storage_pb2.ResponseString(hash_id=hash_)

    def is_hash_id_in_memory(self, request, context):
        """
        :param request: storage_pb2.HashIdRequest(hash_id=hash_id)
        :param context: None
        :return: boolean as storage_pb2.ResponseBoolean
        """
        if DEBUG:
            logger.debug("[storage manager] is hash id in memory called")

        hash_exists = self.memory_manager.hash_id_exists(request.hash_id)
        return storage_pb2.ResponseBoolean(success=hash_exists)

    def is_hash_id_in_memory_non_rpc(self, hash_id):
        """
        :param hash_id: input string
        :return: hash_id found in memory as boolean
        """
        return self.memory_manager.hash_id_exists(hash_id)

    def download_list_of_data_chunks_non_rpc(self, hash_id):
        """
        :param hash_id: input string
        :return: list of data chunks
        """
        return self.memory_manager.get_data(hash_id)

