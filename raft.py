#implement raft and 2pc
#we will have 9 servers
#   3 shards with 3 servers


import socket
import threading

from socket import AF_INET, SOCK_DGRAM
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
from logEntry import logEntry, LETypes
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

crossIndexTable = {}


pendingEntries = []
pendingReqs = set()


DEBUG = False  # or False to turn off debug output
databaseUpdate = 0

databaselock = threading.Lock()
saveDatabaseSem = threading.Semaphore(0)
conductTranLock = threading.Lock()
updatelocks = threading.Lock()

partition = False

tryUnblock = False


#conducts a transaction and adds it to the blockchain also updates the balances
def commitTransaction(sender, receiver, amount):
    global currentTerm, currentLeader,  state, votedFor, votes, log, commitIndex, lastApplied, nextIndex, matchIndex, timer, balance, users, entryKeys, databaseUpdate, locks, mylocks, pendingEntries, pendingReqs, partition, tryUnblock, DEBUG
    balance[sender] -= amount
    balance[receiver] += amount
    debug_print(f"balance of {sender} is {balance[sender]}")
    debug_print(f"balance of {receiver} is {balance[receiver]}")
    saveDatabaseSem.release()

def commitCross(entry, amount):
    global currentTerm, currentLeader,  state, votedFor, votes, log, commitIndex, lastApplied, nextIndex, matchIndex, timer, balance, users, entryKeys, databaseUpdate, locks, mylocks, pendingEntries, pendingReqs, partition, tryUnblock, DEBUG
    balance[entry] += amount
    saveDatabaseSem.release()

def checkLock(ID):
    with updatelocks:
        if(ID in locks):
            return locks[ID] > 0
#adds a lock to an entry
def addLock(ID, send, myLock=False):
    global currentTerm, currentLeader,  state, votedFor, votes, log, commitIndex, lastApplied, nextIndex, matchIndex, timer, balance, users, entryKeys, databaseUpdate, locks, mylocks, pendingEntries, pendingReqs, partition, tryUnblock, DEBUG
    debug_print(f"locking {myLock}: ",ID)
    with updatelocks:
        ID = int(ID)
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
                    if not partition:
                        serverSocket.sendto(f"Lock {ID}".encode(), ('127.0.0.1', i))

#releases a lock to an entry
def releaseLock(ID, send, myLock=False):
    global currentTerm, currentLeader,  state, votedFor, votes, log, commitIndex, lastApplied, nextIndex, matchIndex, timer, balance, users, entryKeys, databaseUpdate, locks, mylocks, pendingEntries, pendingReqs, partition, tryUnblock, DEBUG
    with updatelocks:
        debug_print(f"unlocking {myLock}: ",ID)
        ID = int(ID)
        if myLock:
            if ID not in mylocks:
                return
        if ID in locks:
            locks[ID] = max(0, locks[ID]-1)
            debug_print(f"locks[{ID}], ", locks[ID])
        if myLock:
            if ID in mylocks:
                mylocks.remove(ID)
            saveLocks()
        if send:
            for i in users:
                if i != SERVER_PORT and i != 9000 and i != 8999:
                    if not partition:
                        serverSocket.sendto(f"Unlock {ID}".encode(), ('127.0.0.1', i))
    
def everyoneLock(ID):
    addLock(ID, False, True)
    for i in users:
        if i != SERVER_PORT and i != 9000 and i != 8999:
            if not partition:
                serverSocket.sendto(f"EveryoneLock {ID}".encode(), ('127.0.0.1', i))

def everyoneUnlock(ID):
    releaseLock(ID, False, True)
    for i in users:
        if i != SERVER_PORT and i != 9000 and i != 8999:
            serverSocket.sendto(f"EveryoneUnlock {ID}".encode(), ('127.0.0.1', i))

def saveLocks():
    global currentTerm, currentLeader,  state, votedFor, votes, log, commitIndex, lastApplied, nextIndex, matchIndex, timer, balance, users, entryKeys, databaseUpdate, locks, mylocks, pendingEntries, pendingReqs, partition, tryUnblock, DEBUG
    try:
        with open(f'./saves/locks{MYID}.txt', 'x') as f:
            dump = json.dumps(list(mylocks))
            f.write(dump)
    except FileExistsError:
        with open(f'./saves/locks{MYID}.txt', 'w') as f:

            dump = json.dumps([i for i in mylocks])
            f.write(dump)


def saveLog():
    global currentTerm, currentLeader,  state, votedFor, votes, log, commitIndex, lastApplied, nextIndex, matchIndex, timer, balance, users, entryKeys, databaseUpdate, locks, mylocks, pendingEntries, pendingReqs, partition, tryUnblock, DEBUG
    while(True):
        saveDatabaseSem.acquire()
        debug_print("saving")
        with open(f'./saves/log{MYID}.txt', 'w') as f:
            dump = json.dumps([entry.to_dict() for entry in log])
            f.write(dump)
        with open(f'./saves/lastApp{MYID}.txt', 'w') as f:
            f.write(str(lastApplied))
            f.write("\n")
            f.write(str(commitIndex))
            f.write("\n")
            f.write(str(currentTerm))
        with open(f'./saves/database{MYID}.txt', 'w') as f:
            dump = json.dumps(balance)
            f.write(dump)
        

#starts an election because I want to be a leader :)))        
def startElection():
    global currentTerm, currentLeader,  state, votedFor, votes, log, commitIndex, lastApplied, nextIndex, matchIndex, timer, balance, users, entryKeys, databaseUpdate, locks, mylocks, pendingEntries, pendingReqs, partition, tryUnblock, DEBUG
    if(state == 2):
        return
    #start electionTimer
    timer = electionTimeout
    currentTerm += 1
    debug_print("currentTerm", currentTerm)
    state = 1
    votes = 1
    votedFor = MYID
    for i in users:
        if i != SERVER_PORT and i != 9000 and i != 8999:
            lastLog = log[len(log)-1] if len(log)>0 else logEntry(0,0)
            debug_print(f"Sending RequestVote to {i}")
            if not partition:
                serverSocket.sendto(f"RequestVote {MYID} {currentTerm} {lastLog.index} {lastLog.term}".encode(), ('127.0.0.1', i))

    #wait until currentTerm is the same
    saveCurrentTerm = currentTerm
    #currentTerm should change if rpc 
    while(currentTerm == saveCurrentTerm):
        #if votes is 5 become leader
        if(votes >= 2):
            currentLeader = MYID
            state = 2
            print("Won leader election!")
            #contains the next log index to send to each server
            #this is not an actual index
            nextIndex = {1 + (SHARDID*3): len(log)+1, 2 + (SHARDID*3): len(log)+1, 3 + (SHARDID*3): len(log)+1}
            matchIndex = {1 + (SHARDID*3): 0, 2 + (SHARDID*3): 0, 3 + (SHARDID*3): 0}
            serverSocket.sendto(f"Leader {(MYID-1)//3} {MYID}".encode(), ('127.0.0.1', 9000))
            serverSocket.sendto(f"Leader {(MYID-1)//3} {MYID}".encode(), ('127.0.0.1', 8999))

            threading.Thread(target=heartbeat).start()
            threading.Thread(target=updateCommitIdx).start()
            for i in pendingEntries:
                threading.Thread(target=handle_msg, args=(i[0].encode(), i[1],)).start()
            break



def applyLog(applyIndex):
    global currentTerm, currentLeader,  state, votedFor, votes, log, commitIndex, lastApplied, nextIndex, matchIndex, timer, balance, users, entryKeys, databaseUpdate, locks, mylocks, pendingEntries, pendingReqs, partition, tryUnblock, DEBUG
    #if its a cross shard transaction skip
    #if its a intra apply
    #if its a cross commit apply the cross
    #and the load one
    #and also the fix
    debug_print("applying ", applyIndex)

    if applyIndex.t == LETypes.INTRA.value:
        releaseLock(applyIndex.transaction[0], True, True)
        releaseLock(applyIndex.transaction[1], True, True)
        commitTransaction(applyIndex.transaction[0], applyIndex.transaction[1], applyIndex.transaction[2])
    elif applyIndex.t == LETypes.CROSSDECISION.value:
        crossTrans = log[applyIndex.transaction[0]-1]
        isSender = (crossTrans.transaction[0]-1)//1000 == SHARDID
        if applyIndex.transaction[1] == 1:
            debug_print("applying cross ", crossTrans)
            if isSender:
                commitCross(crossTrans.transaction[0], -1 * crossTrans.transaction[2])
                debug_print(f"balance of {crossTrans.transaction[0]} is {balance[crossTrans.transaction[0]]}")
            else:    
                commitCross(crossTrans.transaction[1], crossTrans.transaction[2])
                debug_print(f"balance of {crossTrans.transaction[1]} is {balance[crossTrans.transaction[1]]}")
    saveDatabaseSem.release()
                    
def updateCommitIdx():
    global currentTerm, currentLeader,  state, votedFor, votes, log, commitIndex, lastApplied, nextIndex, matchIndex, timer, balance, users, entryKeys, databaseUpdate, locks, mylocks, pendingEntries, pendingReqs, partition, tryUnblock, DEBUG
    while(state == 2):
        with databaselock:
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
                            applyLog(applyIndex)
                            lastApplied += 1
                        break


def sendAppendMessage(id, port):
    global currentTerm, currentLeader,  state, votedFor, votes, log, commitIndex, lastApplied, nextIndex, matchIndex, timer, balance, users, entryKeys, databaseUpdate, locks, mylocks, pendingEntries, pendingReqs, partition, tryUnblock, DEBUG
    start = nextIndex[id]
    debug_print("Start ", start)
    #nexxtIndex is the next block idx to send starts from 1
    entries = log[start-1:]
    for entry in entries:
        debug_print(entry)
    prevLog = log[start-2] if start>1 else logEntry(0,0)
    json_data = json.dumps([entry.to_dict() for entry in entries])
    sendMess = f"AppendEntries {currentTerm} {MYID} {prevLog.index} {prevLog.term} {commitIndex}...{json_data}"
    debug_print(sendMess)
    if not partition:
        debug_print("partition is false")
        serverSocket.sendto(sendMess.encode(), ('127.0.0.1', port))


def sendGhostAppend():
    global currentTerm, currentLeader,  state, votedFor, votes, log, commitIndex, lastApplied, nextIndex, matchIndex, timer, balance, users, entryKeys, databaseUpdate, locks, mylocks, pendingEntries, pendingReqs, partition, tryUnblock, DEBUG
    
    entry = logEntry(len(log)+1, currentTerm, (-1, -1, -1), LETypes.GHOST.value)
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

def conductIntra(data, clientPort):
    global currentTerm, currentLeader,  state, votedFor, votes, log, commitIndex, lastApplied, nextIndex, matchIndex, timer, balance, users, entryKeys, databaseUpdate, locks, mylocks, pendingEntries, pendingReqs, partition, tryUnblock, DEBUG

    data = data.decode()
    split = data.split()
    sender = int(split[1])
    receiver = int(split[2])
    amt = int(split[3])
    requestID = int(split[4])
    if(len(split) > 5):
        if(state == 2):
            return
        clientPort = int(split[5])
    if (sender == -1):
        sendGhostAppend()
        return
    # if state == 2:
    #     if(checkLock(sender) or checkLock(receiver)):
    #         serverSocket.sendto(f"IntraResponse No {requestID} {MYID} 0".encode(), ('127.0.0.1', clientPort))
    #         return
    #TODO: possible change, if there is no leader, we can deny the request, just make the leader change faster?
    for i in log:
        if i.requestID == requestID:
            debug_print(f"{sender} {receiver} {amt} {requestID} already in log")
            debug_print("Already in log")
            return
    if(state == 2 and (checkLock(sender) or checkLock(receiver))):
        serverSocket.sendto(f"IntraResponse No {requestID} {MYID} 0".encode(), ('127.0.0.1', clientPort))
        return  
    if(state != 2):
        #server has no one to forward to return no
        pendingEntries.append((data, clientPort))
        #forward to leader
        if(currentLeader != None):
            serverSocket.sendto(f"IntraRequest {sender} {receiver} {amt} {requestID} {clientPort}".encode(), ('127.0.0.1', 9000 + currentLeader))
        return

    conductTranLock.acquire()
    try:
        releasedTranLock = False
        for i in log:
            if i.requestID == requestID:
                debug_print(f"{sender} {receiver} {amt} {requestID} already in log")
                return
        if state != 2:
            return
        debug_print('got passed log check')
        
        #server is a leader
        #check balances
        if(balance[sender] < amt):
            serverSocket.sendto(f"IntraResponse No {requestID} {MYID} 1".encode(), ('127.0.0.1', clientPort))
            return
        if(checkLock(sender) or checkLock(receiver)):
            serverSocket.sendto(f"IntraResponse No {requestID} {MYID} 0".encode(), ('127.0.0.1', clientPort))
            return  
        
        everyoneLock(sender)
        everyoneLock(receiver)
        
        entry = logEntry(len(log)+1, currentTerm, (sender, receiver, amt), LETypes.INTRA.value, requestID)

        #append command to log
        log.append(entry)
        debug_print("entry added ", entry)
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
            if tryUnblock and not releasedTranLock:
                tryUnblock = False
                releasedTranLock = True
                conductTranLock.release()
            continue

        if(log[entry.index-1].term != currentTerm):
            serverSocket.sendto(f"IntraResponse No {requestID} {MYID} 2".encode(), ('127.0.0.1', clientPort))
            releaseLock(sender, False, True)
            releaseLock(receiver, False, True)
            return
        #tell user
        reply = f"IntraResponse Yes {requestID} {MYID}"
        debug_print(f"replying to client {clientPort} ", reply)
        serverSocket.sendto(f"IntraResponse Yes {requestID} {MYID}".encode(), ('127.0.0.1', clientPort))
    finally:
        if conductTranLock.locked() and not releasedTranLock:
            conductTranLock.release()
    

def conductCross(data, clientPort):
    global currentTerm, currentLeader,  state, votedFor, votes, log, commitIndex, lastApplied, nextIndex, matchIndex, timer, balance, users, entryKeys, databaseUpdate, locks, mylocks, pendingEntries, pendingReqs, partition, tryUnblock, DEBUG
    data = data.decode()
    split = data.split()
    sender = int(split[1])
    receiver = int(split[2])
    amount = int(split[3])
    requestID = int(split[4])
    if(len(split) > 5):
        if(state == 2):
            return
        clientPort = int(split[5])

    # if(state == 2):
    #     isSenderShard = (int(sender)-1)//1000 == SHARDID
    #     if isSenderShard and checkLock(sender):
    #             serverSocket.sendto(f"CrossResponse No {requestID} {MYID} 0".encode(), ('127.0.0.1', clientPort))
    #             return
    #     if not isSenderShard and checkLock(receiver):
    #         serverSocket.sendto(f"CrossResponse No {requestID} {MYID} 0".encode(), ('127.0.0.1', clientPort))
    #         return
    #TODO: possible change, if there is no leader, we can deny the request, just make the leader change faster?
    
    #Prechecks...
    for i in log:
        if i.requestID == requestID:
            debug_print(f"{sender} {receiver} {amount} {requestID} already in log")
            return
    
    isSenderShard = (int(sender)-1)//1000 == SHARDID
    #RAFT consists of getting locks and getting consensus
    if state == 2:
        if isSenderShard and checkLock(sender):
                serverSocket.sendto(f"CrossResponse No {requestID} {MYID} 0".encode(), ('127.0.0.1', clientPort))
                return
        if not isSenderShard and checkLock(receiver):
            serverSocket.sendto(f"CrossResponse No {requestID} {MYID} 0".encode(), ('127.0.0.1', clientPort))
            return
    
    if(state != 2):
        pendingEntries.append((data, clientPort))
        if(currentLeader != None):
            serverSocket.sendto(f"CrossRequest {sender} {receiver} {amount} {requestID} {clientPort}".encode(), ('127.0.0.1', 9000 + currentLeader))
        return
    conductTranLock.acquire()
    try:
        releasedTranLock = False
        if state != 2:
            return
        #check the logs again
        for i in log:
            if i.requestID == requestID:
                debug_print(f"{sender} {receiver} {amount} {requestID} already in log")
                return
            
        debug_print("isSenderShard", isSenderShard)
        #check balance
        if(isSenderShard):
            if balance[int(sender)] < int(amount):
                serverSocket.sendto(f"CrossResponse No {requestID} {MYID} 1".encode(), ('127.0.0.1', clientPort))
                return
        
        #RAFT consists of getting locks and getting consensus
        if isSenderShard and checkLock(sender):
                serverSocket.sendto(f"CrossResponse No {requestID} {MYID} 0".encode(), ('127.0.0.1', clientPort))
                return
        
        if not isSenderShard and checkLock(receiver):
            serverSocket.sendto(f"CrossResponse No {requestID} {MYID} 0".encode(), ('127.0.0.1', clientPort))
            return
        #lock sender and receiver values
        if isSenderShard:
            everyoneLock(sender)
        else:
            everyoneLock(receiver)
        
        #both sender and receiver need to get consensus, add block
        entry = logEntry(len(log)+1, currentTerm, (sender, receiver, amount), LETypes.CROSS.value, requestID)
        debug_print("entry added ", entry)
        log.append(entry)

        matchIndex[MYID] = len(log)
        nextIndex[MYID] = len(log)+1
        sleep(sendDelay)
        for port,id in users.items():
            if port != SERVER_PORT and port != 9000 and port != 8999:
                debug_print(f"Sending AppendRPC to {port}")
                #if last index >= nextindex
                if(nextIndex[id] <= len(log)):
                    sendAppendMessage(id, port)

        #if got consensus, reply to client

        while(commitIndex < entry.index):
            if tryUnblock and not releasedTranLock:
                tryUnblock = False
                releasedTranLock = True
                conductTranLock.release()
                #release the tran lock
            continue

        if(log[entry.index-1].term != currentTerm):
            if(isSenderShard):
                releaseLock(sender, False, True)
            else:
                releaseLock(receiver, False, True)
            print("Could not get consensus")
            serverSocket.sendto(f"CrossResponse No {requestID} {MYID} 2".encode(), ('127.0.0.1', clientPort))
            
            return
        #wait for client to reply commit block
        reply = f"CrossResponse Yes {requestID} {MYID}"

        debug_print(f"replying to client {clientPort} ", reply)
        serverSocket.sendto(reply.encode(), ('127.0.0.1', clientPort))
        #should tell me what block to commit?
        #release locks
    finally:
        if conductTranLock.locked() and not releasedTranLock:
            conductTranLock.release()



    



#given an index to revert back to, check lastApplied 
def fixLog(prevIdx):
    global currentTerm, currentLeader,  state, votedFor, votes, log, commitIndex, lastApplied, nextIndex, matchIndex, timer, balance, users, entryKeys, databaseUpdate, locks, mylocks, pendingEntries, pendingReqs, partition, tryUnblock, DEBUG
    debug_print("fixing log", prevIdx)
    with databaselock:
            if(lastApplied <= prevIdx):
                log = log[:prevIdx]
            else:
                debug_print("cleaning if fixUP")
                #clean up everything after prevIdx, to lastApplied
                for i in range(prevIdx, lastApplied):
                    #i is actual index in log
                    if log[i].t == LETypes.INTRA.value:
                        
                        balance[log[i].transaction[0]] += log[i].transaction[2]
                        balance[log[i].transaction[1]] -= log[i].transaction[2]
                        debug_print("reverting index ", i)
                        debug_print("reverting intra ", log[i])
                        debug_print("balance of ", log[i].transaction[0], " is ", balance[log[i].transaction[0]])
                        debug_print("balance of ", log[i].transaction[1], " is ", balance[log[i].transaction[1]])

                    elif log[i].t == LETypes.CROSSDECISION.value:

                        revertCrossTran = log[log[i].transaction[0]-1]
                        if log[i].transaction[1] == 1:
                            debug_print("reverting cross ", revertCrossTran)
                            
                            if revertCrossTran.transaction[0] in balance:
                                balance[revertCrossTran.transaction[0]] += revertCrossTran.transaction[2]
                                debug_print(f"balance of {revertCrossTran.transaction[0]} is {balance[revertCrossTran.transaction[0]]}")
                            else:
                                balance[revertCrossTran.transaction[1]] -= revertCrossTran.transaction[2]
                                debug_print(f"balance of {revertCrossTran.transaction[1]} is {balance[revertCrossTran.transaction[1]]}")
                log = log[:prevIdx]
                debug_print("cleaned up")
                debug_print("log after cleanup")
                for i in log:
                    debug_print(i)
                debug_print("lastApplied", lastApplied)
                lastApplied = prevIdx
                debug_print("lastApplied", lastApplied)


            # commitIndex = min(commitIndex, prevIdx)
    debug_print("done fixing log")
        


def handle_msg(data, port):
    global currentTerm, currentLeader,  state, votedFor, votes, log, commitIndex, lastApplied, nextIndex, matchIndex, timer, balance, users, entryKeys, databaseUpdate, locks, mylocks, pendingEntries, pendingReqs, partition, tryUnblock, DEBUG
    # simulate 3 seconds message-passing delay
    # decode byte data into a string
    data = data.decode()

    if(data=="locks"):
        print(locks)
        print(lastApplied)
        print(commitIndex)
    if data == "exit":
                serverSocket.close()
                _exit(0)
    if(data == "PrintDatastore"):
        print("Datastore:\n")
        for i in range(0, commitIndex):
            print(log[i])
    if(data[:12] == "PrintBalance"):
        entryID = int(data.split()[1])
        debug_print(f"Printbalance entryid {balance[entryID]}")
        serverSocket.sendto(f"PrintBalance {balance[entryID]}".encode(), ('127.0.0.1', port))
    
    if partition:
        return

    # debug_print(data)
    # echo message to console
    if(data[:2] == "Hi"):
        if(port not in users.keys()):
            users[port] = int(data[3:])
            debug_print("connected to", users[port])
        tryUnblock = True
        serverSocket.sendto(f"Done {sys.argv[1]}".encode(), ('127.0.0.1', port))
    
    if(data[:4] == "Done"):
        if(port not in users.keys()):
            users[port] = int(data[5:])
            debug_print("connected to", users[port])

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
        debug_print("votedFor,   ", votedFor)
        if (((currentTerm == candTerm and (votedFor == None or votedFor == candId)) or (currentTerm < candTerm)) and (candLastTerm > currLastLog.term or (candLastTerm == currLastLog.term and candLastIdx >= currLastLog.index))):
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
        threading.Thread(target=conductIntra, args=(data.encode(),port,)).start()
    
    elif(data[:12] == "CrossRequest"):
        debug_print(data)
        threading.Thread(target=conductCross, args=(data.encode(),port,)).start()
    elif(data[:13] == "AppendEntries"):
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
        if(reqTerm > currentTerm):
            debug_print(f"   | Message received from {sender}: {data}")
            debug_print("reqTerm/CurrTerm", reqTerm, currentTerm)
            state = 0
            currentTerm = reqTerm
            currentLeader = reqID
            timer = electionTimeout
            debug_print("becoming follower")
        #its a heartbeat
        if(entries == ''):
            pendingEntries  = []
            timer = electionTimeout
            # debug_print("Entries is empty") its an heartbeat
            serverSocket.sendto(f"AppendResponse {currentTerm} No -1".encode(), ('127.0.0.1', port))
        else:
            debug_print("Received: ", data)
            debug_print("reqPrevIdx", reqPrevIdx)
            #get the prevIdx log to check
            
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
            else:
                checkLog = log[reqPrevIdx-1] if (len(log) > 0 and reqPrevIdx > 0) else logEntry(0,0) 
                if(checkLog.term != reqPrevTerm):
                    debug_print("case3")
                    sending = f"AppendResponse {currentTerm} No {reqPrevIdx}"
                    debug_print(sending)
                    serverSocket.sendto(sending.encode(), ('127.0.0.1', port))
                else:
                    #delete everything past index
                    debug_print("ReqPrevIdx", reqPrevIdx)
                    fixLog(reqPrevIdx)
                    entries = [logEntry.from_dict(item) for item in entries]
                    debug_print("Entries: ", entries)
                    for i in entries:
                        log.append(i)
                    
                    for i in log:
                        debug_print(i)
                    sending = f"AppendResponse {currentTerm} Yes {len(log)}"
                    serverSocket.sendto(sending.encode(), ('127.0.0.1', port))
            #set commit index based on what the leader knows 
        with databaselock:
            if(reqComIdx > commitIndex and state != 2):
                commitIndex = min(reqComIdx, len(log))
                while(commitIndex > lastApplied):
                    debug_print("APPLIED INDEX", lastApplied)
                    applyIndex = log[lastApplied]
                    applyLog(applyIndex)
                    lastApplied += 1

    elif(data[:14] == "AppendResponse"):
        split = data.split()
        term = int(split[1])
        response = split[2]
        nextLog = int(split[3])
        # debug_print("nextLog ",nextLog)
        # debug_print(f"{sender}: {data}")
    
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
    
    elif(data[:13] == "CrossDecision"):
        split = data.split()
        answer = split[1]
        requestID = int(split[2])
        senderID = int(split[3])
        receiverID = int(split[4])
        logIndex = None
        crossLog = None
        for i in log:
            if i.t == LETypes.CROSS.value and i.requestID == requestID:
                crossLog = i
                logIndex = i.index
        if(state == 2):
            if(answer == "Yes"):
                #addentry
                # logindex = None
                if logIndex == None:
                    debug_print("CrossDecision: logIndex not found")
                    return
                logentry = logEntry(len(log)+1, currentTerm, (logIndex, 1, 0), LETypes.CROSSDECISION.value, requestID)
            else:
                if logIndex == None:
                    debug_print("CrossDecision: logIndex not found")
                    return
                logentry = logEntry(len(log)+1, currentTerm, (logIndex, 0, 0), LETypes.CROSSDECISION.value, requestID)
            log.append(logentry)
            matchIndex[MYID] = len(log)
            nextIndex[MYID] = len(log)+1
            #lets try to commit a rando block
            threading.Thread(target=conductIntra, args=("0 -1 -1 -1 -1".encode(),port,)).start()
            

        isSender = (senderID-1)//1000 == SHARDID
        if isSender:
            releaseLock(senderID, True, True)
        else:
            releaseLock(receiverID, True, True)
        
    elif(data[:4] == "Lock"):
        ID = int(data.split()[1])
        addLock(ID, False)
    elif(data[:6] == "Unlock"):
        ID = int(data.split()[1])
        releaseLock(ID, False)
    elif(data[:12] == "EveryoneLock"):
        ID = int(data.split()[1])
        #achnowledge the incoming lock
        addLock(ID, False, False)
        #tell everyone that you have the lock as well
        addLock(ID, True, True)
    elif(data[:14] == "EveryoneUnlock"):
        ID = int(data.split()[1])
        #acknowledge the incoming unlock
        releaseLock(ID, False, False)
        #tell everyone that you released the lock
        releaseLock(ID, True, True)
   


def get_user_input():
    global currentTerm, currentLeader,  state, votedFor, votes, log, commitIndex, lastApplied, nextIndex, matchIndex, timer, balance, users, entryKeys, databaseUpdate, locks, mylocks, pendingEntries, pendingReqs, partition, tryUnblock, DEBUG
    while True:
        try:
            userInput = input()
            # close all sockets before exiting
            if userInput == "exit":
                serverSocket.close()
                _exit(0)
            elif userInput == "locks":
                print(locks)
                print(lastApplied)
                print(commitIndex)
            elif userInput == "logs":
                for i in log:
                    print(i)
            elif userInput == "balance":
                print(balance)
            elif(userInput == "makeleader"):
                threading.Thread(target=startElection).start()
            elif(userInput == "partition"):
                print("Partitioned!")
                partition = True
            elif(userInput == "unpartition"):

                users = {8999:-1, 9000:0, 9001+ (SHARDID*3):1 +(SHARDID*3),9002+ (SHARDID*3): 2 +(SHARDID*3),9003+ (SHARDID*3): 3+(SHARDID*3)}
                for i in users:
                    if i != SERVER_PORT:
                        debug_print("connecting to ", i)
                        serverSocket.sendto(f"Hi {sys.argv[1]}".encode(), ('127.0.0.1', i))
                print("Unpartitioned!")
                timer = electionTimeout
                partition = False
            elif(userInput == "debug"):
                DEBUG = not DEBUG
            
            
        except EOFError or AttributeError or IndexError:
            continue

#starts elections if haven't heard in a bit
def handleElectionTimeout():
    global currentTerm, currentLeader,  state, votedFor, votes, log, commitIndex, lastApplied, nextIndex, matchIndex, timer, balance, users, entryKeys, databaseUpdate, locks, mylocks, pendingEntries, pendingReqs, partition, tryUnblock, DEBUG
    while(1):
        if (timer > 0 or state == 2): continue
        #if haven't received a rpc yet (leader has crashed) and isn't in the middle of an election start an election

        debug_print("starting election")
        threading.Thread(target=startElection).start()
        timer = electionTimeout


def subtractTimer():
    global currentTerm, currentLeader,  state, votedFor, votes, log, commitIndex, lastApplied, nextIndex, matchIndex, timer, balance, users, entryKeys, databaseUpdate, locks, mylocks, pendingEntries, pendingReqs, partition, tryUnblock, DEBUG
    while(1):
        sleep(.03)
        timer -= .03


def sendHeartbeat():
    global currentTerm, currentLeader,  state, votedFor, votes, log, commitIndex, lastApplied, nextIndex, matchIndex, timer, balance, users, entryKeys, databaseUpdate, locks, mylocks, pendingEntries, pendingReqs, partition, tryUnblock, DEBUG

    for port in users:
            if port != SERVER_PORT and port != 9000 and port != 8999:
                # debug_print(f"Sending Heartbeat to {port}")
                if(len(log) > 1):
                    prevLog = log[len(log)-2]
                else:
                    prevLog = logEntry(0,0)
                if not partition:
                    serverSocket.sendto(f"AppendEntries {currentTerm} {MYID} {prevLog.index} {prevLog.term} {commitIndex}...".encode(), ('127.0.0.1', port))

def heartbeat():#when become leader call heartbeat
    while(state == 2):
            sleep(.2)
            threading.Thread(target=sendHeartbeat).start()

#initializes connections betweens servers
def connect():
    global users, DEBUG
    #client is 9000
    users = {8999:-1, 9000:0, 9001+ (SHARDID*3):1 +(SHARDID*3),9002+ (SHARDID*3): 2 +(SHARDID*3),9003+ (SHARDID*3): 3+(SHARDID*3)}
    for i in users:
        if i != SERVER_PORT:
            debug_print("connecting to ", i)
            serverSocket.sendto(f"Hi {sys.argv[1]}".encode(), ('127.0.0.1', i))

def debug_print(*args, **kwargs):
    """
    Print only if DEBUG is True.
    Accepts any arguments that a normal debug_print function would.
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
    print("Starting...")
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
                debug_print(lastApplied)
                commitIndex = int(f.readline())
                debug_print(commitIndex)
                currentTerm = int(f.readline())
                debug_print(currentTerm)
    except FileNotFoundError:
        with open(f'./saves/lastApp{MYID}.txt', 'x') as f:
            ...
    try:
        with open(f'./saves/locks{MYID}.txt', 'r') as f:
            if f.read() != '':
                f.seek(0)
                mylocks = set(json.load(f))
                copyofmylocks = mylocks.copy()
                for i in copyofmylocks: 
                    releaseLock(i, True, True)

    except FileNotFoundError:
        with open(f'./saves/locks{MYID}.txt', 'x') as f:
            ...
    # threading.Thread(target=initialize).start()
    serverSocket = socket(AF_INET, SOCK_DGRAM)
    serverSocket.bind(('', SERVER_PORT))
    electionTimeout = (MYID % 3) * 2 + 2
    timer = electionTimeout
    debug_print("Election Timeout: ", electionTimeout)
    debug_print("Server is up and running")
    serverSocket.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
    threading.Thread(target=connect).start()
    threading.Thread(target=subtractTimer).start()
    threading.Thread(target=handleElectionTimeout).start()
    threading.Thread(target=saveLog).start()
    threading.Thread(target=get_user_input).start()

    while True:
        try:
            message, clientAddress = serverSocket.recvfrom(2048)
            threading.Thread(target=handle_msg, args = (message, clientAddress[1])).start()
        except ConnectionResetError:
            continue
