import json
import socket

def start_replica():
    # get node self IP
    serverAddressPort = ("EDIT_IP", 21000)
    UDPClientSocket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
    print(serverAddressPort)
    dict = {}
    message = json.dumps({"IPaddress": "<EDIT_IP>", "gossip": False, "Dictionary": dict, "BlackListedNodes": []})
    UDPClientSocket.sendto(message.encode(), serverAddressPort)