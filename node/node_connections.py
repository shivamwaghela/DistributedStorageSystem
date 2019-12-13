import grpc
import globals

class NodeConnections:
    """
    Stores all node connections
    """
    MAX_CONNECTIONS = 4

    def __init__(self):
        self.connection_dict = {}

    def is_full(self):
        """
        Checks if the connections have reached capacity
        :return: True if length of connection_list equals MAX_CONNECTIONS
                else False
        """
        if len(self.connection_dict) < self.MAX_CONNECTIONS:
            return False
        else:
            return True

    def add_connection(self, connection):
        """
        Adds node connection
        :param connection: Connection object
        :return: Returns True if connection was added successfully, False otherwise
        """
        if connection.node_position in self.connection_dict:
            return False

        for item in self.connection_dict.items():
            if item[1].node_ip == connection.node_ip:
                return False

        with globals.lock:
            self.connection_dict[connection.node_position] = connection

        return True

    def remove_connection(self, node_position):
        """
        Adds node connection
        :param node_position: Connection position (Left/Right/Top/Bottom)
        :return: Returns True if connection was removed successfully, False otherwise
        """
        if node_position in self.connection_dict:
            with globals.lock:
                del self.connection_dict[node_position]
            return True

        return False

    def get_connection_status(self):
        """
        Checks status of channels
        :return: Dictionary containing status of connections
        """
        connection_status = {}
        for node_position, connection in self.connection_dict:
            try:
                grpc.channel_ready_future(connection.channel).result(timeout=1)
            except grpc.FutureTimeoutError:
                connection_status[node_position] = False
            connection_status[node_position] = True

        return connection_status
