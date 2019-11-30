
def get_neighbor_coordinates(node_pos):
    """
    Calculates node's neighbor's cartesian coordinates
    :param node_pos: tuple containing node's coordinates
    :return: neighbor coordinates as a dictionary
    """
    x, y = node_pos
    top = (x - 1, y)
    bottom = (x + 1, y)
    left = (x, y - 1)
    right = (x, y + 1)

    return {"top": top, "bottom": bottom, "left": left, "right": right}
