#!/usr/bin/env python
# -*- coding: utf-8
import math
import time
import os
import sys
import logging

sys.path.append("../" + os.path.dirname(os.path.realpath(__file__)))
sys.path.append(os.path.dirname(os.path.realpath(__file__)) + '/utils/')
sys.path.append(os.path.dirname(os.path.realpath(__file__)) + '/generated/')

from page import Page
from space_binary_tree import SpaceBinaryTree

DEBUG = 0
ADD = "add"
REMOVE = "remove"
PRINT_LIST_BREAK = 5

class MemoryManager:
    '''
    Atrributes
    '''

    memory_tracker = None  # key: memory_hash_id, value: list_of_pages_ids>
    list_of_all_pages = []
    total_number_of_pages = 0
    total_memory_size = 0
    page_size = 0
    list_of_pages_used = []
    fragmentation_threshold = 0
    pages_free = None

    '''
    Methods
    '''
    # Constructor
    def __init__(self, total_memory_size, page_size):

        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)
        self.memory_tracker={}

        # define the attributes
        self.total_memory_size = total_memory_size
        self.page_size = page_size

        self.total_number_of_pages = math.floor(self.total_memory_size / self.page_size)

        for i in range(self.total_number_of_pages):
            self.list_of_all_pages.append(Page(self.page_size))

        self.pages_free = SpaceBinaryTree(self.total_memory_size, self.total_number_of_pages)
        self.logger.info("Number of pages available in memory %s of size %s bytes." % (len(self.list_of_all_pages),
                                                                                           self.page_size))

    def save_stream_data(self, data, index):
        self.list_of_all_pages[index].put_data(data)
        return

    # def put_data(self, memory_id, data_chunks, num_of_chunks):
    def put_data(self, data_chunks, hash_id, number_of_chunks, is_single_chunk):
        '''
        :param data_chunks:
        :param hash_id:
        :param chunk_size:
        :param data_size:
        :return:
        '''

        if hash_id in self.memory_tracker:
            self.logger.info("The following hash_id exist and it will be overwritten: %s." % hash_id)
            # delete data associated this this hash_id before we save the new one
            self.delete_data(hash_id)
        else:
            self.logger.info("\nWriting new data with hash_id: %s." % hash_id)

        pages_needed = number_of_chunks #self.get_number_of_pages_needed(chunk_size, number_of_chunks)
        if DEBUG:
            print("[memory manager] start time for getting pages")
            start_find_pages_time = time.time()
        # find available blocks of pages to save the data
        target_list_indexes = self.find_n_available_pages(pages_needed)

        if DEBUG:
            for i, index in enumerate(target_list_indexes):
                if isinstance(target_list_indexes[i], list):
                    print("[memory manager] this should not be a list")
                    print("[memory manager] first element is : {}".format(target_list_indexes[i][0]))
                    break

                print("[memory manager] target list of indexes : {}".format(target_list_indexes[index]))
                if i >= PRINT_LIST_BREAK:
                    break
            total_find_pages_time = round(time.time() - start_find_pages_time, 6)
            print("[memory manager] total time getting pages: {}".format(total_find_pages_time))

        #tracking the time it takes for the write operation
        if DEBUG:
            print("[memory manager] start time for writing data")
            start_write_data = time.time()

        # save the data in pages
        try:

        # index_counter = 0
            if not is_single_chunk:
                temp_data = list(data_chunks)
                temp = [self.save_stream_data(c, target_list_indexes[i]) for i, c in enumerate(temp_data)]
            #     for c in data_chunks:
            #         self.list_of_all_pages[target_list_indexes[index_counter]].put_data(c)
            #         index_counter = index_counter + 1
            else:
                self.list_of_all_pages[target_list_indexes[0]].put_data(data_chunks)
            #     self.list_of_all_pages[target_list_indexes[index_counter]].put_data(data_chunks)
            #     index_counter = index_counter + 1
        except:
            raise

        if DEBUG:
            total_time_write_data = round(time.time() - start_write_data, 6)
            print("[memory manager] total time writing data: {}".format(total_time_write_data))

        #assert index_counter == pages_needed  # make sure we use all the pages we needed

        # update the list with used pages
        # self.list_of_pages_used.extend.(target_list_indexes)
        # update memory dic
        self.memory_tracker[hash_id] = target_list_indexes
        if DEBUG:
            self.logger.info("Successfully saved the data in %s pages. Bytes written: %s. Took %s seconds." %
              (pages_needed, pages_needed * self.page_size, total_time_write_data))
            self.logger.info("Free pages left: %s. Bytes left: %s" % (self.get_number_of_pages_available(),
                                                                      self.get_available_memory_bytes()))

        return True

    # def get_number_of_pages_needed(self, chunk_size, data_size):
    #     if self.page_size != chunk_size:
    #         message = "Page set in the server is different than the chunk size specified. Please send the data with " \
    #                   "the correct chunk from the client side. This will be supported in the future."
    #         raise Exception(message)
    #     else:
    #         return math.ceil(data_size / chunk_size)  # taking ceiling to account for last page being partially occupied

    def get_number_of_pages_available(self):
        return self.total_number_of_pages - len(self.list_of_pages_used)

    def get_available_memory_bytes(self):
        return self.get_number_of_pages_available() * self.page_size

    def get_data(self, hash_id):
        '''
        :param hash_id:
        :return:
        '''

        pages_to_return = []

        data_pages = self.memory_tracker[hash_id]
        start_read_data = time.time()

        for index in data_pages:
            pages_to_return.append(self.list_of_all_pages[index].get_data())

        total_time_read_data = round(time.time() - start_read_data, 6)

        self.logger.info("Returning: data for %s composed of %s pages. Took %s sec" %
              (hash_id, str(len(pages_to_return)), total_time_read_data))

        return pages_to_return

    def update_data(self, data, memory_id):
        '''
        :param data:
        :param memory_id:
        :return:
        '''
        # TODO
        pass

    # this function is very slow, we need to improve it. (This will use a tree)
    def find_n_available_pages(self, n):
        self.logger.info("Looking for %s available pages... " % n)
        start = time.time()
        list_indexes_to_used = self.pages_free.get_available_space(n)
        total_time = round(time.time() - start, 6)

        if len(list_indexes_to_used) != n:
            raise Exception("Not enough pages available to save the data. Took %s seconds." % total_time)
        else:
            self.logger.info("Enough pages available to save the data. Took %s seconds." % total_time)

        return list_indexes_to_used

    def delete_data(self, hash_id):
        '''
        :param hash_id:
        :return:
        '''
        # find the data from the memory storage
        if DEBUG:
            print("[memory manager] Entering the delete data function")
        #get the list of pages used
        old_pages_list = self.memory_tracker.pop(hash_id)
        if DEBUG:
            print("[memory manager] getting old pages list: {}".format(old_pages_list))
            print("[memory manager] len of list to remove: {}".format(len(old_pages_list)))

        #add pages to the free pages structure
        self.pages_free.set_empty_space(len(old_pages_list),old_pages_list)

        if DEBUG:
            print("[memory manager] called set_empty_space | num_of slots: {}, free_pages: {}".format(len(old_pages_list),
                                                                                                  old_pages_list))
        #delete the memory tracker
        # del self.memory_tracker[hash_id]  # delete the mapping
        # self.list_of_pages_used = [x for x in self.list_of_pages_used if x not in old_pages_list]
        # we may also need to delete the data from the actual Pages() in list_of_all_pages
        self.logger.info("Successfully deleted hash_id: %s." % hash_id)

    def get_stored_hashes_list(self):
        return list(self.memory_tracker.keys())

    def hash_id_exists(self, hash_id):
        if hash_id in self.memory_tracker:
            return True
        else:
            return False

    def defragment_data(self):
        '''
        :param data:
        :param list_of_pages:
        :return:
        '''
        # TODO
        pass

    def partition_data(self, data, list_of_pages):
        '''
        :param data:
        :param list_of_pages:
        :return:
        '''
        # TODO
        pass

    def update_binary_tree(self):
        '''
        :return:
        '''
        # TODO
        pass
