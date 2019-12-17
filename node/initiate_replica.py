import json
import socket
import globals
import time
from gossip_of_gossip import GossipProtocol
def start_replica():
    # get node self IP
    serverAddressPort = (globals.my_ip, 21000)
    UDPClientSocket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
    print("INSIDE GOSSIP", serverAddressPort)
    dict = {}
    message = json.dumps({"IPaddress": globals.my_ip, "gossip": False, "Dictionary": dict, "BlackListedNodes": []})
    GossipProtocol()
    time.sleep(5)
    UDPClientSocket.sendto(message.encode(), serverAddressPort)