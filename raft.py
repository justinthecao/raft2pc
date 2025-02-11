#implement raft and 2pc
#we will have 9 servers
#   3 shards with 3 servers


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
import random 
import json
#latest term server has seen



currentTerm = 0
#currentLeader
currentLeader = None
#current state (will be either 0,1,2: followers, candidate, leader)
state = 0
#candidateID that you voted for this term
votedFor = None 
#used for election
votes = 0
#index starts at 1 (entry contains command for datastore and term when entry was received by leader)
log = []
#index of highest log entry known to be committed
commitIndex = 0
#index of highest log entry applied to data store
lastApplied = 0

#for each server, index of the next log entry to send to that server
nextIndex = None 
#for each server, index of highest log entry known to be replicated on server
matchIndex = None


#used for electionTimeout
electionTimeout = 0 #static
timer = None
sendDelay = .5


#majority table
reqVotes = {}

balance = None #datastore

#mapping for ports to id but we just use 900012 hard mapped for now
users = {}

ids = set()



def getBalance():
    return None, [0]
    

class logEntry():
    #type of entry
    t = None
    #index of entry
    index = None
    #term of the logentry
    term = None
    #transactions are in the form (sender, receiver, amount)
    transaction = None

    def __init__(self, index, term, trans=None, t=None):
        self.t = t
        self.index = index
        self.term = term
        self.transaction = trans  # Transaction is a tuple (sender, receiver, amount)

    def to_dict(self):
        """Convert the logEntry object to a dictionary."""
        return {
            "t": self.t,
            "index": self.index,
            "term": self.term,
            "transaction": self.transaction
        }

    @staticmethod
    def from_dict(data):
        """Create a logEntry object from a dictionary."""
        return logEntry(
            index=data["index"],
            term=data["term"],
            trans=data["transaction"],
            t=data.get("t")
        )

    def __str__(self):
        return (
            "(\n"
            + f"\tIndex: {self.index}\n"
            + f"\tTerm: {self.term}\n"
            + f"\tSender: {self.transaction[0]}\n"
            + f"\tReceiver: {self.transaction[1]}\n"
            + f"\tAmount: {self.transaction[2]}\n"
            + ")\n"
        )
        

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
    global currentTerm, currentLeader,  state, votedFor, votes, log, commitIndex, lastApplied, nextIndex, matchIndex, receivedAppendRPC, grantedVote, timer
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


#starts an election because I want to be a leader :)))        
def startElection():
    global currentTerm, currentLeader,  state, votedFor, votes, log, commitIndex, lastApplied, nextIndex, matchIndex, receivedAppendRPC, grantedVote, timer
    #start electionTimer
    timer = electionTimeout
    votes = 0
    currentTerm += 1
    state = 1
    votes += 1
    votedFor = MYID
    #send requestvotes  to everyone TODO:
    for i in users:
        if i != SERVER_PORT:
            sleep(sendDelay)
            lastLog = log[len(log)-1] if len(log)>0 else logEntry(0,0)
            print(f"Sending RequestVote to {i}")
            serverSocket.sendto(f"RequestVote {MYID} {currentTerm} {lastLog.index} {lastLog.term}".encode(), ('127.0.0.1', i))


    #wait until currentTerm is the same
    saveCurrentTerm = currentTerm
    #currentTerm should change if rpc 
    while(currentTerm == saveCurrentTerm):
        #if votes is 5 become leader
        if(votes >= 2):
            print("Im leader")
            currentLeader = MYID
            state = 2
            nextIndex = {1 + (SHARDID*3): len(log)+1, 2 + (SHARDID*3): len(log)+1, 3 + (SHARDID*3): len(log)+1}
            matchIndex = {1 + (SHARDID*3): 0, 2 + (SHARDID*3): 0, 3 + (SHARDID*3): 0}
            threading.Thread(target=heartbeat).start()
            break
   


def sendHeartbeat():
    global currentTerm, currentLeader,  state, votedFor, votes, log, commitIndex, lastApplied, nextIndex, matchIndex, receivedAppendRPC, grantedVote, timer

    for port in users:
            if port != SERVER_PORT:
                print(f"Sending Heartbeat to {port}")
                if(len(log) > 1):
                    prevLog = log[len(log)-2]
                else:
                    prevLog = logEntry(0,0)

                serverSocket.sendto(f"AppendEntries {currentTerm} {MYID} {len(log)-1} {prevLog.term} {commitIndex} {0}...".encode(), ('127.0.0.1', port))

def getID():
    i = 0
    while(i in ids):
        i+=1
    return i
    



def conductIntra(split):
    sender = int(split[1])
    receiver = int(split[2])
    amt = int(split[3])
    if(state != 2):
        serverSocket.sendto(f"IntraRequest {sender} {receiver} {amt}", 9000 + currentLeader) #TODO: what to do if currentLeader is none here
    #TODO:
    #get locks
    #check balances
    if(balance[sender] < amt):
        #fail
        #return
        ...
    
    entry = logEntry(len(log)+1, currentTerm, (sender, receiver, amt), "intra")
    #append command to log
    log.append(entry)
    #checking for majority table
    transactionID = getID()
    reqVotes[transactionID] = 1
    
    #send append rpcs to followers

    for port,id in users:
        if port != SERVER_PORT:
            sleep(sendDelay)
            print(f"Sending AppendRPC to {port}")
            #if last index >= nextindex
            if(nextIndex[id] <= len(log)):
                start = nextIndex[id]
                entries = log[start-1:]
                prevLog = log[len(log)-2] if len(log)>1 else logEntry(0,0)
                json_data = json.dumps([entry.to_dict() for entry in entries])
                serverSocket.sendto(f"AppendEntries {currentTerm} {MYID} {prevLog.index} {prevLog.term} {commitIndex} {transactionID}...{json_data}".encode(), ('127.0.0.1', port))
    
    #if heard from majority commited
    
    while(reqVotes[transactionID]==1):
        continue
    
    #commit

    
    #notify follows of committed entries
            
    #tell user
    


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
    global currentTerm, currentLeader,  state, votedFor, votes, log, commitIndex, lastApplied, nextIndex, matchIndex, receivedAppendRPC, grantedVote, timer
    # simulate 3 seconds message-passing delay
    # decode byte data into a string
    data = data.decode()
    # echo message to console
    if(data[:2] == "Hi"):
        if(port not in users.keys()):
            users[port] = int(data[3:])
            serverSocket.sendto(f"Hi {sys.argv[1]}".encode(), ('127.0.0.1', port))
            print("connected to", users[port])
    sender = int(users[port])

    if(data[:11] == "RequestVote"):
        split = data.split()
        candId = int(split[1])
        candTerm = int(split[2])
        candLastIdx = int(split[3])
        candLastTerm = int(split[4])
        currLastLog = log[len(log)-1] if len(log)>0 else logEntry(0,0)

        if ((currentTerm < candTerm) or (currentTerm == candTerm and (votedFor == None or votedFor == candId) and (candLastTerm > currLastLog.term or (candLastTerm == currLastLog.term and candLastIdx >= currLastLog.index)))):
            timer = electionTimeout
            currentTerm = candTerm
            votedFor = candId
            #become follower
            state= 0
            votes = 0
            sleep(sendDelay)
            print("voting yes")
            serverSocket.sendto(f"VoteResponse {currentTerm} Yes".encode(), ('127.0.0.1', port))
        else:
            sleep(sendDelay)
            print("voting no")
            serverSocket.sendto(f"VoteResponse {currentTerm} No".encode(), ('127.0.0.1', port))

    if(data[:12] == "VoteResponse"):
        split = data.split()
        responseTerm = int(split[1])
        voteGranted = split[2]
        if(responseTerm > currentTerm):
            currentTerm = responseTerm
            state = 0
        elif (voteGranted == "Yes"):
            votes += 1        
            




    #when I get a request, add request to quee and send a reply
    if(data[:12] == "IntraRequest"):
        threading.Thread(target=conductIntra, args=(data.split())).start()
    
    if(data[:13] == "AppendEntries"):
        timer = electionTimeout
        print("timer updated to ", timer)
        split = data.split("...")
        values = split[0]
        entries = json.load(split[1]) if split[1] != '' else ''
        print(entries)
        valuesplit = values.split()
        reqTerm =  int(valuesplit[1])
        reqID = int(valuesplit[2])
        reqPrevIdx = int(valuesplit[3])
        reqPrevTerm =   int(valuesplit[4])
        reqComIdx = int(valuesplit[5])
        transID = int(valuesplit[6])
        timer = electionTimeout
        if(reqTerm >= currentTerm):
            state = 0
            currentTerm = reqTerm
            currentLeader = reqID

        checkLog = log[reqPrevIdx-1] if len(log) > 0 else logEntry(0,0) 
        if(entries == '' or reqTerm < currentTerm or reqPrevIdx > len(log) or (checkLog.term != reqPrevTerm)):
            serverSocket.sendto(f"AppendResponse {currentTerm} No {reqPrevIdx} {transID}".encode(), ('127.0.0.1', port))
        else:
            #delete everything past index
            log = log[:reqPrevIdx]
            for i in entries:
                log.append(i)
            serverSocket.sendto(f"AppendResponse {currentTerm} Yes {len(log)} {transID}".encode(), ('127.0.0.1', port))
        if(reqComIdx > commitIndex): commitIndex = min(reqComIdx, len(log))
        
        
    if(data[:14] == "AppendResponse"):
        split = data.split()
        term = int(split[1])
        response = split[2]
        nextLog = int(split[3])
        transID = int(split[4])
        if(term > currentTerm):
            state = 0
            currentTerm = term
        elif(response == "Yes"):
            nextIndex[sender] = nextLog + 1
            matchIndex[sender] = nextLog
            reqVotes[transID] += 1
        elif(response == "No"):
            nextIndex[sender] = nextLog - 1
            #ask them again with a smaller nextIndex
        

        


        

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


#starts elections if haven't heard in a bit
def handleElectionTimeout():
    global timer
    while(1):
        if (timer > 0 or state == 2): continue
        #if haven't received a rpc yet (leader has crashed) and isn't in the middle of an election start an election
        print("starting election")
        threading.Thread(target=startElection).start()
        timer = electionTimeout


def subtractTimer():
    global timer
    while(1):
        sleep(.03)
        timer -= .03


def heartbeat():#TODO: when become leader call heartbeat
    while(state == 2):
            sleep(1)
            threading.Thread(target=sendHeartbeat).start()


#initializes connections betweens servers
def connect():
    sleep(3)
    #client is 9000
    users = [9001+ (SHARDID*3),9002+ (SHARDID*3),9003+ (SHARDID*3)]
    for i in users:
        if i != SERVER_PORT:
            print("connecting to", i)
            serverSocket.sendto(f"Hi {sys.argv[1]}".encode(), ('127.0.0.1', i))

#inits
if __name__ == "__main__":
    SERVER_IP = gethostname()
    MYID = int(sys.argv[1])
    SERVER_PORT = int(sys.argv[2])
    SHARDID = (MYID-1)//3
    print(SERVER_PORT)
    balance = {1 + (SHARDID*3): 10, 2 + (SHARDID*3): 10, 3 + (SHARDID*3): 10}
    # threading.Thread(target=initialize).start()
    serverSocket = socket(AF_INET, SOCK_DGRAM)
    serverSocket.bind(('', SERVER_PORT))
    electionTimeout = random.random()*5 + 5
    timer = electionTimeout
    print("Server is up and running")
    serverSocket.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
    threading.Thread(target=get_user_input).start()
    threading.Thread(target=connect).start()

    threading.Thread(target=subtractTimer).start()
    threading.Thread(target=handleElectionTimeout).start()
    print("Current balance is $10")
    while True:
        try:
            message, clientAddress = serverSocket.recvfrom(2048)
            threading.Thread(target=handle_msg, args = (message, clientAddress[1])).start()
        except ConnectionResetError:
            continue