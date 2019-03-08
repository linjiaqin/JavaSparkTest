#!/usr/bin/python
# -*- coding: UTF-8 -*-

import socket
import time
import threading

def catNumber(c,d):
    i = 0
    while i < 20:
        c.send("haha " + str(i) +"\n")
        i = i+1
        time.sleep(1)

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
host = "127.0.0.1"
port = 9999
s.bind( (host, port))

s.listen(5)
i = 0


#while True: #if you want multiply threads to deal request, erase the '#', and add tab to the codes below
c,addr = s.accept()
print 'bind address', addr
t = threading.Thread(target=catNumber, args=(c,"ljq"))
t.start()
t.join()
s.close()



