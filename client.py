import socket
import threading

from socket import *
from os import _exit
from sys import stdout
from time import sleep
import time
import os
from hashlib import sha256
import array as arr
import collections
import sys
import heapq
import argparse


users = {}
leaders = {0: None, 1: None, 2:None}
# requestDone = 0
ids = set()

DEBUG = False
random = [0,0,0]

def debug_print(*args, **kwargs):
    """
    Print only if DEBUG is True.
    Accepts any arguments that a normal print function would.
    """
    if DEBUG:
        print(*args, **kwargs)

def getID():
    global ids
    i = 0
    while(i in ids):
        i+=1
    ids.add(i)
    return i
    
def removeID(i):
    global ids
    if i in ids:
        ids.remove(i)

#send request to all other servers
def sendReq(sender, receiver, amount):
    global random
    #send request to all other servers	
    if((sender-1)//1000 == (receiver-1)//1000):
        shard = (sender-1)//1000
        #intra transaction
        #send
        if(leaders[shard] == None):
            print(9001 + random[shard] + shard*3)
            port = 9001 + random[shard] + shard*3
            serverSocket.sendto(f"IntraRequest {sender} {receiver} {amount}".encode(), ('127.0.0.1', port))
            random[shard] = (random[shard] + 1) % 3
        else:
            serverSocket.sendto(f"IntraRequest {sender} {receiver} {amount}".encode(), ('127.0.0.1', 9000 + leaders[shard]))
    else:
        #cross transaction
        #send request to leader of sender
        ...

def get_user_input():
    while True:
        try:
            userInput = input()
            # close all sockets before exiting
            if userInput[:8] == "Transfer":
                split = userInput.split()
                sender = int(split[1])
                receiver = int(split[2])
                amount = int(split[3])
                sendReq(sender, receiver, amount)
                print("Transfer Initiated...")
            
            elif userInput[:12] == "FileTransfer":
                filename = userInput.split()[1]
                print(filename)
                try:
                    with open(filename) as f:
                        for line in f:
                            split = line.split(',')
                            sender = int(split[0][1:])
                            receiver = int(split[1])
                            amount = int(split[2].split(')')[0])
                            print("amount", amount)
                            sendReq(sender, receiver, amount)
                            print("Transfer Initiated...")
                except FileNotFoundError:
                    print("File not found")
            elif userInput == "PrintBalance":
                ...
            elif userInput == "PrintDatastore":
                for i in users:
                    serverSocket.sendto(f"PrintDatastore".encode(), ('127.0.0.1', i))
            elif userInput == "PrintBalance":
                entryID = int(userInput.split()[1])
                for i  in range(9001 + 3*(entryID-1)//1000, 9004 + 3*(entryID-1)//1000):
                    serverSocket.sendto(f"PrintBalance {entryID}".encode(), ('127.0.0.1', i))

            elif userInput == "exit":
                serverSocket.close()
                _exit(0)
        except EOFError or AttributeError or IndexError or ValueError:
            continue



def handle_msg(data, port):
    global users, leaders, random
    # simulate 3 seconds message-passing delay
    # decode byte data into a string
    data = data.decode()
    # echo message to console
    if(data[:2] == "Hi"):
        users[port] = int(data[3:])
        serverSocket.sendto(f"Done {MYID}".encode(), ('127.0.0.1', port))
        print("connected to", users[port])
    
    if(data[:4] == "Done"):
        if(port not in users.keys()):
            users[port] = int(data[5:])
            print("connected to", users[port])

    sender = int(users[port])

    
            
    if(data[:15] == "RequestResponse"):
        data = data.split()
        leader = int(data[2])
        if(data[1] == "Yes"):
            print("Request approved")
        if(data[1] == "No"):
            print("Request denied")
        shard = (sender-1)//1000
        leaders[shard] = leader
    
    if(data[:6] == "Leader"):
        data = data.split()
        debug_print(data)
        shard = int(data[1])
        leader = int(data[2])
        leaders[shard] = leader
        debug_print(f"Leader of shard {shard} is {leader}")

def connect():
    users = [9001,9002,9003,9004,9005,9006,9007,9008,9009]
    for i in users:
        if i != SERVER_PORT:
            print("connecting to", i)
            serverSocket.sendto(f"Hi {MYID}".encode(), ('127.0.0.1', i))

def threadOffInputs():
    while True:
        try:
            message, clientAddress = serverSocket.recvfrom(2048)
            threading.Thread(target=handle_msg, args = (message, clientAddress[1])).start()
        except ConnectionResetError:
            continue

if __name__ == "__main__":
    SERVER_IP = gethostname()
    MYID = 0
    SERVER_PORT = 9000

    debug_print(SERVER_PORT)
    parser = argparse.ArgumentParser(description="Run client")
    parser.add_argument("--debug", action="store_true", help="Turn on debugger statements")
    parser.add_argument("--second", action="store_true", help="Makes a second client")
    args = parser.parse_args()
    if args.debug:
        DEBUG = True
    if args.second:
        MYID = -1
        SERVER_PORT = 8999
    # threading.Thread(target=initialize).start()
    serverSocket = socket(AF_INET, SOCK_DGRAM)
    serverSocket.bind(('', SERVER_PORT))
    debug_print("Server is up and running")
    serverSocket.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
    threading.Thread(target=get_user_input).start()
    connect()
    threading.Thread(target=threadOffInputs).start()
    
    