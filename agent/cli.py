#!//usr/bin/env python

import socket
import sys


def main():
    socket_name = '/tmp/statistic_adapter.ipc'
    client = socket.socket(socket.AF_UNIX,socket.SOCK_DGRAM)
    for i in sys.argv:
        if sys.argv.index(i) == 0:
            continue
        client.sendto(i, socket_name)


####

main()
