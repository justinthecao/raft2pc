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
import argparse
from logEntry import logEntry
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
sendDelay = 3

#memory balance
balance = {}
#mapping for ports to id but we just use 900012 hard mapped for now
users = {}
entryKeys = {}

#locks are updated when a transaction starts
#if leader sees that a variable has a lock >0, it rejects
#if not...
#when a process gets a lock it tells everyone
#when a process releases a lock it tells everyone
#save the locks so on startup, we can tell everyone i released the lock
mylocks = set()
locks = {}


DEBUG = False  # or False to turn off debug output
databaseUpdate = 0

#conducts a transaction and adds it to the blockchain also updates the balances
def commitTransaction(sender, receiver, amount):
    global currentTerm, currentLeader,  state, votedFor, votes, log, commitIndex, lastApplied, nextIndex, matchIndex, timer, balance, users, entryKeys, databaseUpdate, locks, mylocks
    balance[sender] -= amount
    balance[receiver] += amount
    databaseUpdate += 1

def checkLock(ID):
    if(ID in locks):
        return locks[ID] > 0
#adds a lock to an entry
def addLock(ID, send, myLock=False):
    global currentTerm, currentLeader,  state, votedFor, votes, log, commitIndex, lastApplied, nextIndex, matchIndex, timer, balance, users, entryKeys, databaseUpdate, locks, mylocks
    print("locking: ",ID)
    if ID not in locks:
        locks[ID] = 1
    else:
        locks[ID] = min(3, locks[ID]+1)
    if myLock:
        mylocks.add(ID)
        saveLocks()
    if send:
        for i in users:
            if i != SERVER_PORT and i != 9000 and i != 8999:
                serverSocket.sendto(f"Lock {ID}".encode(), ('127.0.0.1', i))

#releases a lock to an entry
def releaseLock(ID, send, myLock=False):
    global currentTerm, currentLeader,  state, votedFor, votes, log, commitIndex, lastApplied, nextIndex, matchIndex, timer, balance, users, entryKeys, databaseUpdate, locks, mylocks
    if ID in locks:
        locks[ID] = max(0, locks[ID]-1)
    if myLock:
        if ID in mylocks:
            mylocks.remove(ID)
        saveLocks()
    if send:
        for i in users:
            if i != SERVER_PORT and i != 9000 and i != 8999:
                serverSocket.sendto(f"Unlock {ID}".encode(), ('127.0.0.1', i))
    
def everyoneLock(ID):
    addLock(ID, False, True)
    for i in users:
        if i != SERVER_PORT and i != 9000 and i != 8999:
            serverSocket.sendto(f"EveryoneLock {ID}".encode(), ('127.0.0.1', i))

def everyoneUnlock(ID):
    releaseLock(ID, False, True)
    for i in users:
        if i != SERVER_PORT and i != 9000 and i != 8999:
            serverSocket.sendto(f"EveryoneUnlock {ID}".encode(), ('127.0.0.1', i))

def saveLocks():
    global currentTerm, currentLeader,  state, votedFor, votes, log, commitIndex, lastApplied, nextIndex, matchIndex, timer, balance, users, entryKeys, databaseUpdate, locks, mylocks
    try:
        with open(f'./saves/locks{MYID}.txt', 'x') as f:
            dump = json.dumps(mylocks)
            f.write(dump)
    except FileExistsError:
        with open(f'./saves/locks{MYID}.txt', 'w') as f:

            dump = json.dumps([i for i in mylocks])
            f.write(dump)


def saveLog():
    global currentTerm, currentLeader,  state, votedFor, votes, log, commitIndex, lastApplied, nextIndex, matchIndex, timer, balance, users, entryKeys, databaseUpdate, locks, mylocks
    while(True):
        saveLastApplied = lastApplied
        while (lastApplied == saveLastApplied):
            continue
        with open(f'./saves/log{MYID}.txt', 'w') as f:
            dump = json.dumps([entry.to_dict() for entry in log])
            f.write(dump)
        with open(f'./saves/lastApp{MYID}.txt', 'w') as f:
            f.write(str(lastApplied))
            f.write("\n")
            f.write(str(commitIndex))

def saveDatabase():
    global currentTerm, currentLeader,  state, votedFor, votes, log, commitIndex, lastApplied, nextIndex, matchIndex, timer, balance, users, entryKeys, databaseUpdate, locks, mylocks

    while(True):
        saveDatabaseUpdate = databaseUpdate
        while (databaseUpdate == saveDatabaseUpdate):
            continue
        with open(f'./saves/database{MYID}.txt', 'w') as f:
            dump = json.dumps(balance)
            f.write(dump)

#starts an election because I want to be a leader :)))        
def startElection():
    global currentTerm, currentLeader,  state, votedFor, votes, log, commitIndex, lastApplied, nextIndex, matchIndex, timer, balance, users, entryKeys, databaseUpdate, locks, mylocks
    #start electionTimer
    timer = electionTimeout
    currentTerm += 1
    state = 1
    votes = 1
    votedFor = MYID
    for i in users:
        if i != SERVER_PORT and i != 9000 and i != 8999:
            lastLog = log[len(log)-1] if len(log)>0 else logEntry(0,0)
            debug_print(f"Sending RequestVote to {i}")
            serverSocket.sendto(f"RequestVote {MYID} {currentTerm} {lastLog.index} {lastLog.term}".encode(), ('127.0.0.1', i))

    #wait until currentTerm is the same
    saveCurrentTerm = currentTerm
    #currentTerm should change if rpc 
    while(currentTerm == saveCurrentTerm):
        #if votes is 5 become leader
        if(votes >= 2):
            currentLeader = MYID
            state = 2
            print("I am the leader")
            nextIndex = {1 + (SHARDID*3): len(log)+1, 2 + (SHARDID*3): len(log)+1, 3 + (SHARDID*3): len(log)+1}
            matchIndex = {1 + (SHARDID*3): 0, 2 + (SHARDID*3): 0, 3 + (SHARDID*3): 0}
            serverSocket.sendto(f"Leader {(MYID-1)//3} {MYID}".encode(), ('127.0.0.1', 9000))
            serverSocket.sendto(f"Leader {(MYID-1)//3} {MYID}".encode(), ('127.0.0.1', 8999))

            threading.Thread(target=heartbeat).start()
            threading.Thread(target=updateCommitIdx).start()
            break

def updateCommitIdx():
    global currentTerm, currentLeader,  state, votedFor, votes, log, commitIndex, lastApplied, nextIndex, matchIndex, timer, balance, users, entryKeys, databaseUpdate, locks, mylocks
    while(state == 2):
        # If there exists an N such that N > commitIndex, a majority of matchIndex[i] ≥ N, and log[N].term == currentTerm: set commitIndex = N
        for i in range(len(log), commitIndex, -1):
            # debug_print("Checking for commit index", i)
            if(log[i-1].term == currentTerm):
                count = 0
                for j in matchIndex:
                    if(matchIndex[j] >= i):
                        count += 1
                if(count >= 2):
                    commitIndex = i
                    while(commitIndex > lastApplied):
                        debug_print("CommitIndex", commitIndex)
                        debug_print("APPLIED INDEX", lastApplied)
                        applyIndex = log[lastApplied]
                        commitTransaction(applyIndex.transaction[0], applyIndex.transaction[1], applyIndex.transaction[2])
                        lastApplied += 1
                    break


def sendAppendMessage(id, port):
    global currentTerm, currentLeader,  state, votedFor, votes, log, commitIndex, lastApplied, nextIndex, matchIndex, timer, balance, users, entryKeys, databaseUpdate, locks, mylocks
    start = nextIndex[id]
    debug_print("Start ", start)
    entries = log[start-1:]
    for entry in entries:
        debug_print(entry)
    prevLog = log[start-2] if start>1 else logEntry(0,0)
    json_data = json.dumps([entry.to_dict() for entry in entries])
    sendMess = f"AppendEntries {currentTerm} {MYID} {prevLog.index} {prevLog.term} {commitIndex}...{json_data}"
    debug_print(sendMess)
    serverSocket.sendto(sendMess.encode(), ('127.0.0.1', port))


def conductIntra(data, clientPort):
    global currentTerm, currentLeader,  state, votedFor, votes, log, commitIndex, lastApplied, nextIndex, matchIndex, timer, balance, users, entryKeys, databaseUpdate, locks, mylocks
    data = data.decode()
    split = data.split()
    sender = int(split[1])
    receiver = int(split[2])
    amt = int(split[3])
    if(len(split) > 4):
        clientPort = int(split[4])
    
    #check balances
    if(balance[sender] < amt):
        serverSocket.sendto(f"RequestResponse No {MYID}".encode(), ('127.0.0.1', clientPort))
        return

    #check locks
    if(checkLock(sender) or checkLock(receiver)):
        serverSocket.sendto(f"RequestResponse No {MYID}".encode(), ('127.0.0.1', clientPort))
        return
    everyoneLock(sender)
    everyoneLock(receiver)
    

    if(state != 2):
        if(currentLeader == None):
            serverSocket.sendto(f"RequestResponse No {MYID}".encode(), ('127.0.0.1', clientPort))
            return
        serverSocket.sendto(f"IntraRequest {sender} {receiver} {amt} {clientPort}".encode(), ('127.0.0.1', 9000 + currentLeader))
        return
    
    entry = logEntry(len(log)+1, currentTerm, (sender, receiver, amt), "I")
    #append command to log
    log.append(entry)
    #update nextIndex and matchIndex
    matchIndex[MYID] = len(log)
    nextIndex[MYID] = len(log)+1
    #send append rpcs to followers
    sleep(sendDelay)
    for port,id in users.items():
        if port != SERVER_PORT and port != 9000 and port != 8999:
            debug_print(f"Sending AppendRPC to {port}")
            #if last index >= nextindex
            if(nextIndex[id] <= len(log)):
                sendAppendMessage(id, port)
    #wait until you have committed the index
    while(commitIndex < entry.index):
        continue
    #TODO: release locks    
    everyoneUnlock(sender)
    everyoneUnlock(receiver)
    #tell user
    serverSocket.sendto(f"RequestResponse Yes {MYID}".encode(), ('127.0.0.1', clientPort))
    


#given an index to revert back to, check lastApplied 
def fixLog(prevIdx):
    global currentTerm, currentLeader,  state, votedFor, votes, log, commitIndex, lastApplied, nextIndex, matchIndex, timer, balance, users, entryKeys, databaseUpdate, locks, mylocks
    if(lastApplied <= prevIdx):
        log = log[:prevIdx]
    else:
        #clean up everything after prevIdx, to lastApplied
        for i in range(prevIdx, lastApplied-1):
            #i is actual index in log
            balance[log[i].transaction[0]] += log[i].transaction[2]
            balance[log[i].transaction[1]] -= log[i].transaction[2]
        log = log[:prevIdx]
        


def handle_msg(data, port):
    global currentTerm, currentLeader,  state, votedFor, votes, log, commitIndex, lastApplied, nextIndex, matchIndex, timer, balance, users, entryKeys, databaseUpdate, locks, mylocks
    # simulate 3 seconds message-passing delay
    # decode byte data into a string
    data = data.decode()
    debug_print(data)
    # echo message to console
    if(data[:2] == "Hi"):
        if(port not in users.keys()):
            users[port] = int(data[3:])
            print("connected to", users[port])
        serverSocket.sendto(f"Done {sys.argv[1]}".encode(), ('127.0.0.1', port))
    
    if(data[:4] == "Done"):
        if(port not in users.keys()):
            users[port] = int(data[5:])
            print("connected to", users[port])
    if(port not in users.keys()):
        return
    sender = int(users[port])

    if(data[:11] == "RequestVote"):
        split = data.split()
        candId = int(split[1])
        candTerm = int(split[2])
        candLastIdx = int(split[3])
        candLastTerm = int(split[4])
        currLastLog = log[len(log)-1] if len(log)>0 else logEntry(0,0)
        if (currentTerm <= candTerm and (votedFor == None or votedFor == candId) and (candLastTerm > currLastLog.term or (candLastTerm == currLastLog.term and candLastIdx >= currLastLog.index))):
            timer = electionTimeout
            currentTerm = candTerm
            votedFor = candId
            #become follower
            state= 0
            votes = 0
            debug_print("voting yes")
            serverSocket.sendto(f"VoteResponse {currentTerm} Yes".encode(), ('127.0.0.1', port))
        else:
            debug_print("voting no")
            serverSocket.sendto(f"VoteResponse {currentTerm} No".encode(), ('127.0.0.1', port))

    elif(data[:12] == "VoteResponse"):
        split = data.split()
        responseTerm = int(split[1])
        voteGranted = split[2]
        if(responseTerm > currentTerm):
            currentTerm = responseTerm
            state = 0
        elif (voteGranted == "Yes"):
            votes += 1        

    #when I get a request, add request to quee and send a reply
    elif(data[:12] == "IntraRequest"):
        debug_print(data)
        threading.Thread(target=conductIntra, args=(data.encode(),port,)).start()
    
    elif(data[:13] == "AppendEntries"):
        # debug_print(f"   | Message received from {sender}: {data}")
        split = data.split("...")
        values = split[0]
        entries = json.loads(split[1]) if split[1] != '' else ''
        valuesplit = values.split()
        reqTerm =  int(valuesplit[1])
        reqID = int(valuesplit[2])
        reqPrevIdx = int(valuesplit[3])
        reqPrevTerm =   int(valuesplit[4])
        reqComIdx = int(valuesplit[5])
        #if term is greater than currentTerm, become follower
        if(reqTerm >= currentTerm):
            state = 0
            currentTerm = reqTerm
            currentLeader = reqID
            timer = electionTimeout
        #its a heartbeat
        if(entries == ''):
            # debug_print("Entries is empty") its an heartbeat
            serverSocket.sendto(f"AppendResponse {currentTerm} No -1".encode(), ('127.0.0.1', port))
        else:
            debug_print("Received: ", data)
            
            #get the prevIdx log to check
            checkLog = log[reqPrevIdx-1] if (len(log) > 0 and reqPrevIdx > 0) else logEntry(0,0) 
            if(reqTerm < currentTerm):
                debug_print("case1")
                sending = f"AppendResponse {currentTerm} No {reqPrevIdx}"
                debug_print(sending)
                serverSocket.sendto(sending.encode(), ('127.0.0.1', port))
            elif(reqPrevIdx > len(log)):
                debug_print("case2")
                sending = f"AppendResponse {currentTerm} No {reqPrevIdx}"
                debug_print(sending)
                serverSocket.sendto(sending.encode(), ('127.0.0.1', port))
            elif(checkLog.term != reqPrevTerm):
                debug_print("case3")
                sending = f"AppendResponse {currentTerm} No {reqPrevIdx}"
                debug_print(sending)
                serverSocket.sendto(sending.encode(), ('127.0.0.1', port))
            else:
                #delete everything past index
                debug_print("ReqPrevIdx", reqPrevIdx)
                fixLog(reqPrevIdx)
                entries = [logEntry.from_dict(item) for item in entries]
                for i in entries:
                    log.append(i)
                for i in log:
                    debug_print(i)
                sending = f"AppendResponse {currentTerm} Yes {len(log)}"
                serverSocket.sendto(sending.encode(), ('127.0.0.1', port))
            #set commit index based on what the leader knows 
        if(reqComIdx > commitIndex and state != 2):
            commitIndex = min(reqComIdx, len(log))
            while(commitIndex > lastApplied):
                debug_print("APPLIED INDEX", lastApplied)
                applyIndex = log[lastApplied]
                commitTransaction(applyIndex.transaction[0], applyIndex.transaction[1], applyIndex.transaction[2])
                lastApplied += 1

    elif(data[:14] == "AppendResponse"):
        split = data.split()
        term = int(split[1])
        response = split[2]
        nextLog = int(split[3])
        # debug_print("nextLog ",nextLog)
        debug_print(f"{sender}: {data}")
    
        if(term > currentTerm):
            debug_print("becoming follower")
            state = 0
            currentTerm = term
        elif(nextLog < 0):
            ...
        elif(response == "Yes"):
            # debug_print("nextLog ",nextLog)
            nextIndex[sender] = nextLog + 1
            matchIndex[sender] = nextLog
        elif(response == "No"):
            nextIndex[sender] = nextLog
            sendAppendMessage(sender, port)
            #ask them again with a smaller nextIndex
    elif(data == "PrintDatastore"):
        print("Datastore:\n")
        for i in range(0, commitIndex):
            print(log[i])
    elif(data[:12] == "PrintBalance"):
        entryID = int(data.split()[1])
        serverSocket.sendto(f"PrintBalance {balance[entryID]}".encode(), ('127.0.0.1', port))
    elif(data == "makeleader"):
        threading.Thread(target=startElection).start()
    elif(data[:4] == "Lock"):
        ID = int(data.split()[1])
        addLock(ID, False)
    elif(data[:6] == "Unlock"):
        ID = int(data.split()[1])
        releaseLock(ID, False)
    elif(data[:12] == "EveryoneLock"):
        ID = int(data.split()[1])
        #achnowledge the incoming lock
        addLock(ID, False, True)
        #tell everyone that you have the lock as well
        addLock(ID, True, False)
    elif(data[:14] == "EveryoneUnlock"):
        ID = int(data.split()[1])
        #acknowledge the incoming unlock
        releaseLock(ID, False, True)
        #tell everyone that you released the lock
        releaseLock(ID, True, False)
    elif data == "exit":
                serverSocket.close()
                _exit(0)


def get_user_input():
    while True:
        try:
            userInput = input()
            # close all sockets before exiting
            if userInput == "exit":
                serverSocket.close()
                _exit(0)
            elif userInput == "locks":
                print(locks)
        except EOFError or AttributeError or IndexError:
            continue

#starts elections if haven't heard in a bit
def handleElectionTimeout():
    global currentTerm, currentLeader,  state, votedFor, votes, log, commitIndex, lastApplied, nextIndex, matchIndex, timer, balance, users, entryKeys, databaseUpdate, locks, mylocks
    while(1):
        if (timer > 0 or state == 2): continue
        #if haven't received a rpc yet (leader has crashed) and isn't in the middle of an election start an election
        debug_print("starting election")
        threading.Thread(target=startElection).start()
        timer = electionTimeout


def subtractTimer():
    global currentTerm, currentLeader,  state, votedFor, votes, log, commitIndex, lastApplied, nextIndex, matchIndex, timer, balance, users, entryKeys, databaseUpdate, locks, mylocks
    while(1):
        sleep(.03)
        timer -= .03


def sendHeartbeat():
    global currentTerm, currentLeader,  state, votedFor, votes, log, commitIndex, lastApplied, nextIndex, matchIndex, timer, balance, users, entryKeys, databaseUpdate, locks, mylocks

    for port in users:
            if port != SERVER_PORT and port != 9000 and port != 8999:
                # debug_print(f"Sending Heartbeat to {port}")
                if(len(log) > 1):
                    prevLog = log[len(log)-2]
                else:
                    prevLog = logEntry(0,0)
                print(f"Heartbeat {commitIndex}...")
                serverSocket.sendto(f"AppendEntries {currentTerm} {MYID} {prevLog.index} {prevLog.term} {commitIndex}...".encode(), ('127.0.0.1', port))

def heartbeat():#when become leader call heartbeat
    while(state == 2):
            sleep(1)
            threading.Thread(target=sendHeartbeat).start()

#initializes connections betweens servers
def connect():
    #client is 9000
    users = [8999, 9000, 9001+ (SHARDID*3),9002+ (SHARDID*3),9003+ (SHARDID*3)]
    for i in users:
        if i != SERVER_PORT:
            print("connecting to ", i)
            serverSocket.sendto(f"Hi {sys.argv[1]}".encode(), ('127.0.0.1', i))

def debug_print(*args, **kwargs):
    """
    Print only if DEBUG is True.
    Accepts any arguments that a normal print function would.
    """
    if DEBUG:
        print(*args, **kwargs)

#inits
if __name__ == "__main__":
    SERVER_IP = gethostname()
    MYID = int(sys.argv[1])
    SERVER_PORT = int(sys.argv[2])
    SHARDID = (MYID-1)//3
    debug_print(SERVER_PORT)
    parser = argparse.ArgumentParser(description="Run server")
    parser.add_argument("--debug", action="store_true", help="Turn on debugger statements")
    args, unknown = parser.parse_known_args()
    if args.debug:
        DEBUG = True
    try:
        with open(f'./saves/database{MYID}.txt', 'r') as f:
            if f.read() == '':
                for i in range(1+SHARDID*1000,1001+SHARDID*1000):
                    balance[i] = 10
            else:
                f.seek(0)
                dict = json.load(f)
                balance = {int(k): int(v) for k, v in dict.items()}
    except FileNotFoundError:
        with open(f'./saves/database{MYID}.txt', 'x') as f:
            ...
        for i in range(1+SHARDID*1000,1001+SHARDID*1000):
            balance[i] = 10
    try:
        with open(f'./saves/log{MYID}.txt', 'r') as f:
            if f.read() != '':
                f.seek(0)
                log = [logEntry.from_dict(item) for item in json.load(f)]
    except FileNotFoundError:
        with open(f'./saves/log{MYID}.txt', 'x') as f:
            ...
    try:
        with open(f'./saves/lastApp{MYID}.txt', 'r') as f:
            if f.read() != '':
                f.seek(0)
                lastApplied = int(f.readline())
                print(lastApplied)
                commitIndex = int(f.readline())
                print(commitIndex)
    except FileNotFoundError:
        with open(f'./saves/lastApp{MYID}.txt', 'x') as f:
            ...
    try:
        with open(f'./saves/locks{MYID}.txt', 'r') as f:
            if f.read() != '':
                f.seek(0)
                mylocks = set(json.load(f))
                for i in mylocks: 
                    releaseLock(i, True)
    except FileNotFoundError:
        with open(f'./saves/locks{MYID}.txt', 'x') as f:
            ...
    # threading.Thread(target=initialize).start()
    serverSocket = socket(AF_INET, SOCK_DGRAM)
    serverSocket.bind(('', SERVER_PORT))
    electionTimeout = random.random()*10 + sendDelay
    timer = electionTimeout
    debug_print("Server is up and running")
    serverSocket.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
    threading.Thread(target=connect).start()
    threading.Thread(target=subtractTimer).start()
    threading.Thread(target=handleElectionTimeout).start()
    threading.Thread(target=saveLog).start()
    threading.Thread(target=get_user_input).start()

    threading.Thread(target=saveDatabase).start()
    while True:
        try:
            message, clientAddress = serverSocket.recvfrom(2048)
            threading.Thread(target=handle_msg, args = (message, clientAddress[1])).start()
        except ConnectionResetError:
            continue