# Fault-Tolerant Distributed Banking System

## Overview

This project implements a **fault-tolerant distributed transaction processing system** for a **simple banking application** using the **Raft consensus algorithm** for intra-shard transactions and the **Two-Phase Commit (2PC) protocol** for cross-shard transactions.

### Key Features:
- **Distributed Architecture:** Data is partitioned into **shards**, each replicated across multiple servers.
- **Consensus Mechanism:** Intra-shard transactions use **Raft** to ensure consistency.
- **Atomic Transactions:** Cross-shard transactions use **2PC** to maintain atomicity.
- **Fault Tolerance:** The system can tolerate failures within predefined limits.
- **Performance Metrics:** Measures **throughput** and **latency** of transactions.

## System Architecture

The system consists of:

1. **Clients** that send transactions in the form of `(x, y, amt)`, where:
   - `x`: Sender account
   - `y`: Receiver account
   - `amt`: Transfer amount
2. **Servers** organized into three **clusters (C1, C2, C3)**, each managing a **shard** of the dataset.
3. **Data Partitioning**:

   | Cluster | Data Items (IDs) |
   |---------|-----------------|
   | **C1**  | 1–1000          |
   | **C2**  | 1001–2000       |
   | **C3**  | 2001–3000       |

## Transaction Processing

- **Intra-Shard Transactions** (within the same shard) are handled using **Raft**.
- **Cross-Shard Transactions** (across different shards) follow **2PC** with clients acting as coordinators.

## Running the System

### 1. Running the Servers

To start the **Raft-based distributed servers**, use the following command:

```bash
python raft.py <server_id> <server_port>
```
Like this
```bash
 python raft.py 8 9008 
```
To start the **Client servers**, use the following command:

```bash
python client.py (--second)
```
With a optional `--second` command to test concurrent transactions

Configurations are hardcoded:\
Shard 1: IDs(1,2,3) Ports(9001,9002,9003\
Shard 2: IDs(4,5,6) Ports(9004,9005,9006)\
Shard 3: IDs(7,8,9) Ports(9007,9008,9009)\
Client 1: Will be defaulted to port 9000\
Client 2: Will be defaulted to port 8999\

### 2. Client Transactions and Commands
#### Transfer
Transfers can be conducted like this
```bash
Transfer 1 2 1
```
#### File Transfer
FileTransfers can be conducted like this
```base
FileTransfer transactions.txt
```
Transactions.txt is expected to be in the format:
```bash
(s1,r1,a1)
(s2,r2,a2)
...
```
#### PrintBalance
```bash
PrintBalance {id}
```
Used to retrieve balances from the corresponding shard servers

#### PrintDatastore
Used to print logs on all servers



### Setup
1. Clone the repository:
   ```bash
   git clone <repo-url>
   cd <repo-folder>
   ```
