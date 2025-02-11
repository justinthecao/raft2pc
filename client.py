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

blockchain = []
balance = {0: 10, 1: 10, 2: 10}
#mapping for ports to id but we just use 900012 hard mapped for now
users = {}
metime = 0 #lamport time
requests = [] #priority queue for requests (time, sender, receiver, amount)

replies = {}


def getBalance():
    return None, [0]
    
#Block chain made up of blocks that contain the hash of the previous block and the transaction to be added
class Block():
    hash = None
    transaction = None
    def __init__(self, ha, trans):
        self.hash = ha
        self.transaction = trans
    def print(self):
        return "(\n" + "\tSender: " + self.transaction[0] + "\n\tReceiver: " + self.transaction[1] + "\n\tAmount: " + self.transaction[2] + "\n\tHash:" + str(self.hash[1]) + "\n)\n"
    #transactions are in the form (sender, receiver, amount)
    def getTransaction(self):
        #transaction are strings
        return self.transaction[0] + self.transaction[1] + self.transaction[2]


#conducts a transaction and adds it to the blockchain also updates the balances
def conductTransaction(sender, receiver, amount):
    global blockchain, balance, metime, users, requests, replies
    if len(blockchain) == 0:
        block = Block((None, None), (str(sender), str(receiver), str(amount)))
    else:
        lastBlock = blockchain[len(blockchain)-1]
        lastBlockHash = str(lastBlock.hash[1])
        lastBlockTransaction = lastBlock.getTransaction()
        block = Block((len(blockchain), sha256((lastBlockHash+lastBlockTransaction).encode('utf-8')).hexdigest()), (str(sender), str(receiver), str(amount)))
    blockchain.append(block)
    balance[sender] -= amount
    balance[receiver] += amount

#tell others to conduct transaction
def sendConductTransaction(receiver, amount):
    global metime
    for i in users:
        if i != SERVER_PORT:
            sleep(3)
            metime += 1
            serverSocket.sendto(f"transfer {receiver} {amount} {metime}".encode(), ('127.0.0.1', i))
            print(f"   |     Local time changed: {metime-1} -> {metime}")
            print(f"   | Message sent to {users[i]}: transfer {receiver} {amount} {metime}")
            

#send request to all other servers
def sendReq(requestTime, receiver, amount):
    global metime
    #send request to all other servers	
    for i in users:
        if i != SERVER_PORT:
            sleep(3)
            metime += 1
            serverSocket.sendto(f"request {requestTime} {receiver} {amount} {metime}".encode(), ('127.0.0.1', i))
            print(f"   |     Local time changed: {metime-1} -> {metime}")
            print(f"   | Message sent to {users[i]}: request {requestTime} {receiver} {amount} {metime}")
            

#send release to all other servers
def release():
    global metime
    #send release to all other servers
    for i in users:
        if i != SERVER_PORT:
            sleep(3)
            metime += 1
            serverSocket.sendto(f"release {metime}".encode(), ('127.0.0.1', i))
            print(f"   |     Local time changed: {metime-1} -> {metime}")
            print(f"   | Message sent to {users[i]}: release {metime}")
            

def get_user_input():
    global blockchain, balance, metime, users, requests, replies
    while True:
        try:
            userInput = input()
            # close all sockets before exiting
            if userInput == "Transfer":
                print("Who do you want to transfer to? Options are users", *[i for i in range(3) if i != MYID])
                try:
                    receiver = int(input())
                except ValueError:
                    print("FAILED: Invalid receiver")
                    continue
                print(f"How much do you want to transfer (current is ${balance[MYID]})?")
                try:
                    amount = int(input())
                except ValueError:
                    print("FAILED: Invalid receiver")
                    continue
                if receiver == MYID:
                    print("FAILED: Cannot transfer to self")
                    continue
                if receiver not in [0,1,2]:
                    print("FAILED: Invalid receiver")
                    continue
                print("Transfer Initiated...")
                requestTime = metime
                heapq.heappush(requests, (int(requestTime), int(MYID), int(receiver), int(amount)))
                replies[requestTime] = 0

                sendReq(requestTime, receiver, amount)
                while ((replies[requestTime] < 2) or (requests[0][:2] != (requestTime, MYID))):
                    continue
                #I have mutex to do the transaction
                if balance[MYID] < amount:
                    print("FAILED: Insufficient Balance")
                else:
                    conductTransaction(MYID, receiver, amount)
                    sendConductTransaction(receiver, amount)
                    print(f"SUCCESS: Your balance is now ${balance[MYID]}")
                release()
                heapq.heappop(requests) #removes the request from the queue

            #print all balances
            elif userInput == "Balance":
                print(f"Your Balance is ${balance[MYID]}")
            elif userInput == "Blockchain":
                stringChain = ""
                if len(blockchain) != 0:
                    for i in blockchain:
                        stringChain += i.print() + "\n"
                    print(stringChain[:-2])
                else:
                    print("[]")
            elif userInput == "Balance Table":
                print(" Balances: \n", *[f" \tUser {i}:${balance[i]}\n" for i in balance])
            elif userInput == "exit":
                serverSocket.close()
                _exit(0)
        except EOFError or AttributeError or IndexError:
            continue



def handle_msg(data, port):
    global blockchain, balance, metime, users, requests, replies
    # simulate 3 seconds message-passing delay
    # decode byte data into a string
    data = data.decode()
    # echo message to console
    if(data[:2] == "Hi"):
        users[port] = int(data[3:])
        print("connected to", users[port])
    sender = int(users[port])

    #when I get a request, add request to quee and send a reply
    if(data[:7] == "request"):
        print(f"   | Message received from {sender}: {data}")
        split = data.split()
        heapq.heappush(requests, (int(split[1]), sender, int(split[2]), int(split[3])))
        incomeTime = int(split[4])
        if incomeTime > metime:
            metime = incomeTime + 1
            print(f"   |     Local time changed: {metime-1} -> {metime}")
        else:
            metime += 1
            print(f"   |     Local time changed: {metime-1} -> {metime}")
        sleep(3)
        metime += 1
        serverSocket.sendto(f"reply {split[1]} {metime}".encode(), ('127.0.0.1', port))
        print(f"   |     Local time changed: {metime-1} -> {metime}")
        print(f"   | Message sent to {sender}: reply {split[1]} {metime}")
        

    #when I get a release, remove the request from the queue
    if(data[:7] == "release"):
        print(f"   | Message received from {sender}: {data}")
        heapq.heappop(requests)
        split = data.split()
        incomeTime = int(split[1])
        if incomeTime > metime:
            metime = incomeTime + 1
            print(f"   |     Local time changed: {metime-1} -> {metime}")
        else:
            metime += 1
            print(f"   |     Local time changed: {metime-1} -> {metime}")
    
    if(data[:8] == "transfer"):
        print(f"   | Message received from {sender}: {data}")
        split = data.split()
        receiver = int(split[1])
        amt = int(split[2])
        incomeTime = int(split[3])
        if incomeTime > metime:
            metime = incomeTime + 1
            print(f"   |     Local time changed: {metime-1} -> {metime}")
        else:
            metime += 1
            print(f"   |     Local time changed: {metime-1} -> {metime}")
        conductTransaction(sender, receiver, amt)
    
    if(data[:5] == "reply"):
        print(f"   | Message received from {sender}: {data}")
        split = data.split()
        replies[int(split[1])] += 1
        incomeTime = int(split[2])
        if incomeTime > metime:
            metime = incomeTime + 1
            print(f"   |     Local time changed: {metime-1} -> {metime}")
        else:
            metime += 1
            print(f"   |     Local time changed: {metime-1} -> {metime}")

    # broadcast to all clients by iterating through each stored connection
    # for sock in out_socks:
    # 	conn = sock[0]
    # 	recv_addr = sock[1]
    # 	# echo message back to client
    # 	try:
    # 		# convert message into bytes and send through socket
    # 		conn.sendall(bytes(f"{addr[1]}: {data}", "utf-8"))
    # 		print(f"sent message to port {recv_addr[1]}", flush=True)
    # 	# handling exception in case trying to send data to a closed connection
    # 	except:
    # 		print(f"exception in sending to port {recv_addr[1]}", flush=True)
    # 		continue

def connect():
    sleep(3)
    users = [9000,9001,9002]
    for i in users:
        if i != SERVER_PORT:
            print("connecting to", i)
            serverSocket.sendto(f"Hi {sys.argv[1]}".encode(), ('127.0.0.1', i))

if __name__ == "__main__":
    SERVER_IP = gethostname()
    MYID = int(sys.argv[1])
    SERVER_PORT = int(sys.argv[2])
    print(SERVER_PORT)
    # threading.Thread(target=initialize).start()
    serverSocket = socket(AF_INET, SOCK_DGRAM)
    serverSocket.bind(('', SERVER_PORT))
    print("Server is up and running")
    serverSocket.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
    threading.Thread(target=get_user_input).start()
    threading.Thread(target=connect).start()
    print("Current balance is $10")
    while True:
        try:
            message, clientAddress = serverSocket.recvfrom(2048)
            threading.Thread(target=handle_msg, args = (message, clientAddress[1])).start()
        except ConnectionResetError:
            continue