import sys


def getip(node):
    nodeid = int(node)
    w2 = nodeid // 1024
    w3 = (nodeid - w2 * 1024) // 8
    w4 = nodeid - w2 * 1024 - w3 * 8 + 1
    nodeip = "172." + str(w2) + '.' + str(w3) + '.' + str(w4)
    return nodeip


if __name__ == '__main__':
    node = sys.argv[1]
    getip(node)
