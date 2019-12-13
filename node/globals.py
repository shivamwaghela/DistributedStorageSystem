"""
Global variables file
"""

import threading
import machine_info
from node_connections import NodeConnections
from node_position import NodePosition

port = None
my_ip = None
my_position = None
my_coordinates = None
node_connections = None
lock = None


def init():
    """
    Definitions of global variables that are used across modules
    """
    global port, my_ip, my_position, my_coordinates, node_connections, lock
    port = 2750
    my_ip = machine_info.get_my_ip()
    my_position = NodePosition.CENTER
    my_coordinates = None
    node_connections = NodeConnections()
    lock = threading.Lock()
