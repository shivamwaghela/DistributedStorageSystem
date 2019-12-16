#!/usr/bin/env python
# -*- coding: utf-8

class TreeNode:
    node_left = None
    node_right= None
    size = 0
    free_pages = []

    def __init__(self, size=0, free_pages=[]):
        self.size = size
        self.free_pages = free_pages

    def set_size(self, size):
        self.size = size

    def set_free_pages(self, pages):
        self.free_pages.append(pages)

    def get_size(self):
        return self.size;

    def get_all_free_pages(self):
        return self.free_pages

    def get_free_pages(self):
        if self.is_node_empty():
            return []
        else:
            ret_val = self.free_pages.pop()[0]
            return ret_val

    def is_node_empty(self):
        if self.free_pages == []:
            return True
        else:
            return False

    def print_free_pages(self):
        max_numbers = 5
        for i, set_of_pages in enumerate(self.free_pages):
            if isinstance(set_of_pages, list):
                temp_list = []
                for j, element in enumerate(set_of_pages):
                    temp_list.append(element)
                    if j > max_numbers:
                        break
                print("[TreeNode] list {}: {}".format(i, temp_list))
