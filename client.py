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

crossTransactionTimeout = 100
intraTransactionTimeout = 100



def debug_print(*args, **kwargs):
    """
    Print only if DEBUG is True.
    Accepts any arguments that a normal print function would.
    """
    if DEBUG:
        print(*args, **kwargs)

def getID():
    global ids
    i = time.time_ns()
    i = int(i*1e9)

    return f"{SERVER_PORT}{i}"
 


class crossHandler:
    id = None
    gotAbort = False
    decisionSent = False
    responses = 0
    sender = None
    receiver = None
    amount = None

    def __init__(self, id, sender, receiver, amount):
        self.id = id
        self.sender = sender
        self.receiver = receiver
        self.amount = amount

class intraHandler:
    id = None
    sender = None
    receiver = None
    timedOut = False
    amount = None
    gotDecision = False

    def __init__(self, id, sender, receiver, amount):
        self.id = id
        self.sender = sender
        self.receiver = receiver
        self.amount = amount

class fileTransactionData:
    count = None
    transactions = None
    sem = None
    def __init__(self, semaphore):
        self.count = 1
        self.transactions = set()
        self.sem = semaphore
    
    
    




crossHandlers = {}
intraHandlers = {}
fileTransHandlers = {}

def timer(requestID):
    sleep(crossTransactionTimeout)
    if not crossHandlers[requestID].decisionSent:
        crossHandlers[requestID].gotAbort = True
        sendCommitAbort(requestID)

def intraTimer(requestID):
    sleep(intraTransactionTimeout)
    if not intraHandlers[requestID].timedOut and not intraHandlers[requestID].gotDecision:
        intraHandlers[requestID].timedOut = True
        if requestID in fileTransHandlers:
            debug_print("releasing")
            fileTransHandlers[requestID].sem.release()
        print("Intra Request Timed Out,", intraHandlers[requestID].receiver, intraHandlers[requestID].sender, intraHandlers[requestID].amount)


#send request to all other servers
def sendReq(sender, receiver, amount, fileTransfer = None):
    global random, crossHandlers, intraHandlers, fileTransHandlers
    #send request to all other servers	
    if((sender-1)//1000 == (receiver-1)//1000):
        debug_print("Intra transaction")
        shard = (sender-1)//1000
        #intra transaction
        #send
        requestID = int(getID())
        # if(leaders[shard] == None):

        #     print(9001 + random[shard] + shard*3)
        #     port = 9001 + random[shard] + shard*3
        #     serverSocket.sendto(f"IntraRequest {sender} {receiver} {amount} {requestID}".encode(), ('127.0.0.1', port))
        #     random[shard] = (random[shard] + 1) % 3
        # else:
        #     serverSocket.sendto(f"IntraRequest {sender} {receiver} {amount} {requestID}".encode(), ('127.0.0.1', 9000 + leaders[shard]))
        for i in range(9001 + 3*shard, 9004 + 3*shard):
            serverSocket.sendto(f"IntraRequest {sender} {receiver} {amount} {requestID}".encode(), ('127.0.0.1', i))
        intraHandlers[requestID] = intraHandler(requestID, sender, receiver, amount)
        if(fileTransfer != None):
            fileTransfer.transactions.add(requestID)
            fileTransHandlers[requestID] = fileTransfer
        threading.Thread(target=intraTimer, args=(requestID,)).start()
        
    else:
        debug_print("Cross transaction")
        #cross transaction
        #send request to leader of sender
        #TODO: sendt o everyone
        shard1 = (sender-1)//1000
        shard2 = (receiver-1)//1000
        print(f"Shard1: {shard1}, Shard2: {shard2}")
        requestID = int(getID())
        # if(leaders[shard1] == None):
        #     print("Leader1 not found")
        #     port = 9001 + random[shard1] + shard1*3
        #     print("Rando port", port)
        #     serverSocket.sendto(f"CrossRequest {sender} {receiver} {amount} {requestID}".encode(), ('127.0.0.1', port))
        #     random[shard1] = (random[shard1] + 1) % 3
        # else:
        #     serverSocket.sendto(f"CrossRequest {sender} {receiver} {amount} {requestID}".encode(), ('127.0.0.1', 9000 + leaders[shard1]))
        for i in range(9001 + 3*shard1, 9004 + 3*shard1):
            serverSocket.sendto(f"CrossRequest {sender} {receiver} {amount} {requestID}".encode(), ('127.0.0.1', i))

        # if(leaders[shard2] == None):
        
        #     print("Leader2 not found")
        #     port = 9001 + random[shard2] + shard2*3
        #     print("Rando port", port)
        #     serverSocket.sendto(f"CrossRequest {sender} {receiver} {amount} {requestID}".encode(), ('127.0.0.1', port))
        #     random[shard2] = (random[shard2] + 1) % 3
        # else:
        #     serverSocket.sendto(f"CrossRequest {sender} {receiver} {amount} {requestID}".encode(), ('127.0.0.1', 9000 + leaders[shard2]))
        for i in range(9001 + 3*shard2, 9004 + 3*shard2):
            serverSocket.sendto(f"CrossRequest {sender} {receiver} {amount} {requestID}".encode(), ('127.0.0.1', i))
        if(fileTransfer != None):
            fileTransfer.transactions.add(requestID)
            fileTransHandlers[requestID] = fileTransfer
        crossHandlers[requestID] = crossHandler(requestID, sender, receiver, amount)
        threading.Thread(target=timer, args=(requestID,)).start()


def sendCommitAbort(requestID):
    global crossHandlers
    if crossHandlers[requestID].decisionSent:
        return
    sender = crossHandlers[requestID].sender
    receiver = crossHandlers[requestID].receiver

    if crossHandlers[requestID].gotAbort:
        if requestID in fileTransHandlers:
            debug_print("releasing")
            fileTransHandlers[requestID].sem.release()
        message = f"CrossDecision No {requestID}"
        
        print("Cross Request Denied,", crossHandlers[requestID].sender, crossHandlers[requestID].receiver, crossHandlers[requestID].amount)
    else:
        if requestID in fileTransHandlers:
            debug_print("releasing")
            fileTransHandlers[requestID].sem.release()
        message = f"CrossDecision Yes {requestID}"
        print("Cross Request Approved,", crossHandlers[requestID].sender, crossHandlers[requestID].receiver, crossHandlers[requestID].amount)
    
    #send to all servers decision
    shard1 = (sender-1)//1000
    shard2 = (receiver-1)//1000
    for i in range(9001 + 3*(shard1), 9004 + 3*(shard1)):
        serverSocket.sendto(message.encode(), ('127.0.0.1', i))
    for i in range(9001 + 3*(shard2), 9004 + 3*(shard2)):
        serverSocket.sendto(message.encode(), ('127.0.0.1', i))
    crossHandlers[requestID].decisionSent = True
    

def timeFileTransaction(fileTransactionData):
    print("starting to time")
    currtime = time.time_ns()
    replies = 0
    print("filecount", fileTransactionData.count)
    while(replies < fileTransactionData.count):
        print("waiting")
        fileTransactionData.sem.acquire()
        print("acquired")
        replies += 1
        print("reply", replies)
        print("filecount", fileTransactionData.count)

    print("HIIII" , (time.time_ns() - currtime)/1e9)

    
def get_user_input():
    global users, leaders, random, crossHandlers, intraHandlers, fileTransHandlers
    while True:
        try:
            userInput = input()
            # close all sockets before exiting
            if userInput[:8] == "Transfer":
                split = userInput.split()
                sender = int(split[1])
                receiver = int(split[2])
                amount = int(split[3])
                if(sender < 1 or sender > 3000 or receiver < 1 or receiver > 3000):
                    print("Invalid sender or receiver")
                    continue
                threading.Thread(target=sendReq, args=(sender, receiver, amount)).start()
                print("Transfer Initiated...")
            
            elif userInput[:12] == "FileTransfer":
                filename = userInput.split()[1]
                print(filename)
                try:
                    
                    with open(filename) as f:
                        fileData = fileTransactionData(threading.Semaphore(0))
                        threading.Thread(target=timeFileTransaction, args = (fileData,)).start()
                        print('File Transfer Initiated...')
                        for line in f:
                            if line == '':
                                continue
                            split = line.split(',')
                            sender = int(split[0][1:])
                            receiver = int(split[1])
                            amount = int(split[2].split(')')[0])
                            print("amount", amount)
                            if(sender < 1 or sender > 3000 or receiver < 1 or receiver > 3000):
                                print("Invalid sender or receiver")
                                continue
                            fileData.count += 1
                            threading.Thread(target=sendReq, args=(sender, receiver, amount, fileData)).start()
                        print("COUNT: ", fileData.count)
                        debug_print("after releasing")
                        fileData.sem.release()

                        

                except FileNotFoundError:
                    print("File not found")
            elif userInput == "PrintDatastore":
                for i in users:
                    serverSocket.sendto(f"PrintDatastore".encode(), ('127.0.0.1', i))
            elif userInput[:12] == "PrintBalance":
                entryID = int(userInput.split()[1])
                for i  in range(9001 + 3*((entryID-1)//1000), 9004 + 3*((entryID-1)//1000)):
                    print("Sending to", i)
                    serverSocket.sendto(f"PrintBalance {entryID}".encode(), ('127.0.0.1', i))

            elif userInput == "exit":
                serverSocket.close()
                _exit(0)
        except Exception as e:
            print(e)
            continue



def handle_msg(data, port):
    global users, leaders, random, intraHandlers, crossHandlers, fileTransHandlers
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

    serverID = int(users[port])

    
            
    if(data[:13] == "IntraResponse"):
        debug_print(f"{data} from {serverID}")
        data = data.split()

        answer = data[1]
        requestID = int(data[2])
    
        if len(data) > 3:
            leader = int(data[3])
            shard = (serverID - 1)//3
            leaders[shard] = leader
        if intraHandlers[requestID].gotDecision:
            return
        if answer == "Yes":
            if intraHandlers[requestID].timedOut:
                print("Intra Request Updated to Committed,", intraHandlers[requestID].sender, intraHandlers[requestID].receiver, intraHandlers[requestID].amount)
            else:
                if requestID in fileTransHandlers:
                    debug_print("releasing")
                    fileTransHandlers[requestID].sem.release()
                intraHandlers[requestID].gotDecision = True
                print("Intra Request Approved, Transaction Details: ", intraHandlers[requestID].sender, intraHandlers[requestID].receiver, intraHandlers[requestID].amount)
        if(answer == "No"):
            error = int(data[4])
            errmsg = ""
            if error == 1:
                errmsg = "Insufficient funds"
            if error == 0:
                errmsg = "Ongoing transaction with either sender or receiver"
            if requestID in fileTransHandlers:
                debug_print("releasing")
                fileTransHandlers[requestID].sem.release()
            intraHandlers[requestID].gotDecision = True
            print(f"Intra Request Denied | {errmsg} |, Transaction Details: ", intraHandlers[requestID].sender, intraHandlers[requestID].receiver, intraHandlers[requestID].amount)
        

    if(data[:13] == "CrossResponse"):
        debug_print(f"{data} from {serverID}")
        data = data.split()
        answer = data[1]
        requestID = int(data[2])
        if len(data) > 3:
            leader = int(data[3])
            shard = (serverID - 1)//3
            leaders[shard] = leader
        crossHandlers[requestID].responses += 1
        debug_print(crossHandlers[requestID].sender, crossHandlers[requestID].receiver, crossHandlers[requestID].amount)
        if(answer == "No"):
            error = int(data[4])
            if error == 1:
                debug_print("CrossRequestVotedNO due to Insufficient funds")
            if error == 0:
                debug_print("CrossRequestVotedNO due to Ongoing transaction with either sender or receiver")
            crossHandlers[requestID].gotAbort = True
        if crossHandlers[requestID].responses == 2:
            sendCommitAbort(requestID)
        


    
    if(data[:6] == "Leader"):
        data = data.split()
        debug_print(data)
        shard = int(data[1])
        if (len(data) > 2):
            leader = int(data[2])
            leaders[shard] = leader
        debug_print(f"Leader of shard {shard} is {leader}")
    
    if(data[:12] == "PrintBalance"):
        print(f"Balance from {serverID}: ", data.split()[1])

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
    
    