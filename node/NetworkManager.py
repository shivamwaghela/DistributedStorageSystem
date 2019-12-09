


class NetworkManager(network_manager_pb2_grpc.NetworkManagerServicer):

    def GetNodeMetaData(self, request, context):
        logger.info("GetNodeMetaData called from: " + request.node_ip)
        file = open("node_meta.txt", "r")
        node_meta_dict = eval(file.readlines()[0])
        file.close()
        return network_manager_pb2.GetNodeMetaDataResponse(node_meta_dict=str(node_meta_dict))

    def UpdateNeighborMetaData(self, request, context):
        logger.info("NetworkManager.UpdateNeighborMetaData: request: " + str(request.node_meta_dict))
        logger.info("NetworkManager.UpdateNeighborMetaData: reading node_meta.txt: ")
        file = open("node_meta.txt", "r")
        node_meta_dict = eval(file.readlines()[0])
        file.close()
        logger.info("NetworkManager.UpdateNeighborMetaData: node_meta_dict: " + str(node_meta_dict))

        logger.info("NetworkManager.UpdateNeighborMetaData: updating node_meta_dict: " + str(node_meta_dict))
        file = open("node_meta.txt", "w")
        node_meta_dict.update(eval(request.node_meta_dict))
        file.write(str(node_meta_dict))
        file.close()
        logger.info("NetworkManager.UpdateNeighborMetaData: node_meta_dict: " + str(node_meta_dict))
        logger.info("NetworkManager.UpdateNeighborMetaData: my_pos: " + str(my_pos))

        return network_manager_pb2.UpdateNeighborMetaDataResponse(status=str(my_pos))


logging.basicConfig(filename='node.log', filemode='w',
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(machine_info.get_my_ip())
logger.setLevel(logging.DEBUG)


