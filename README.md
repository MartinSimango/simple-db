
# Simple-DB 

[![Go Version](https://img.shields.io/badge/Go-1.25.4-00ADD8?style=flat-square&logo=go)](https://golang.org/)
[![License](https://img.shields.io/badge/License-MIT-blue.svg?style=flat-square)](./LICENSE)
[![Protocol Buffers](https://img.shields.io/badge/Protocol%20Buffers-3.0-4285F4?style=flat-square&logo=google)](https://protobuf.dev/)
[![Cobra CLI](https://img.shields.io/badge/CLI-Cobra-green?style=flat-square)](https://cobra.dev/)
[![TCP Server](https://img.shields.io/badge/Network-TCP-orange?style=flat-square)]()

<div align="center">
  <img src="https://github.com/egonelbre/gophers/blob/master/vector/projects/network.svg" alt="Go Gopher Database" width="200"/>
</div>

A simple log-structured key-value database engine implemented in Go. Inspired by the concepts from "Designing Data-Intensive Applications" (DDIA), this database features Write-Ahead Logging (WAL), customizable in-memory tables, and SSTable storage for optimal read/write performance. 

> [!IMPORTANT]
> This project is still a work in progress so core features are still being developed. The features that have been implemented will have a ticked check box next to them.


## 📋 Table of Contents

- [✨ Features](#-features)
- [🏗️ Architecture](#️-architecture)
  - [Core Components](#core-components)
  - [🗃️ Supported MemTable Types](#️-supported-memtable-types)
- [📦 Installation](#-installation)
  - [Prerequisites](#prerequisites)
  - [Install the server](#install-the-server)
  - [Install the db client](#install-the-db-client)
  - [Install the load tester](#install-the-load-tester)
- [🚀 Quick Start](#-quick-start)
  - [Starting the Database Server](#starting-the-database-server)
  - [Using the CLI Client](#using-the-cli-client)
  - [Using the Go Client](#using-the-go-client)
- [⚡ Performance Testing](#-performance-testing)
- [📊 Technical Details](#-technical-details)
  - [Storage Engine](#storage-engine)
  - [Network Protocol](#network-protocol)
  - [Concurrency](#concurrency)
- [📄 License](#-license)

## ✨ Features
- [x] **Concurrent Operations**: TCP server handling multiple client connections
- [x] **Write-Ahead Logging (WAL)**: Ensures data durability using Protocol Buffers
- [x] **In-Memory Tables**: Fast key-value operations with eventual persistence
- [ ] **SSTable Storage**: Sorted String Table implementation for efficient disk storage
- [x] **CLI Interface**: Easy-to-use command-line tools built with Cobra
- [x] **Thread-Safe Operations**: Concurrent operations with Go routines and mutexes
- [x] **Load Testing**: Built-in performance testing tools
- [x] **Recovery Mode**: Automatic recovery from WAL files on startup
- [ ] **Non-blocking Writes**: Support for concurrent writes whilst flushing memtable to SSTable
- [ ] **Multiple WAL Files**: Enable concurrent flushes for better performance
- [ ] **Compaction**: SSTable merging and compaction strategies
- [ ] **Pluggable MemTable Types**: Multiple memtable implementations will be supported
- [ ] **Distributed Database**: Replica support with leader-follower architecture for high availability


## 🏗️ Architecture

Simple-DB implements a log-structured storage engine with the following components:

### Core Components

- **Memory Table**: In-memory sorted map for fast read/write operations
- **Write-Ahead Log**: Persistent transaction log using Protocol Buffers
- **SSTables**: Immutable sorted string tables for long-term storage
- **TCP Server**: High-performance network interface
- **CLI Client**: Command-line interface for database operations



### 🗃️ Supported MemTable Types

- [x] **Hash Map**: Simple Go map implementation (currently implemented)
- [ ] **Red-Black Tree**: Balanced binary search tree for ordered iteration
- [ ] **Skip List**: Probabilistic data structure for fast search operations
- [ ] **AVL Tree**: Self-balancing binary search tree for guaranteed O(log n) operations


## 📦 Installation

### Prerequisites

- Go 1.25.4 or later
- Protocol Buffers compiler (for development)

> [!NOTE]
> Versioning for simple-db will be added when the app has functioning sstable storage.

### Install the server
```bash
go install github.com/MartinSimango/simple-db/cmd/sdb@latest 
```

### Install the db client
```bash
go install github.com/MartinSimango/simple-db/cmd/sdb-cli@latest 

```
### Install the load tester

```bash
go install github.com/MartinSimango/simple-db/cmd/sdb-test@latest 
```

## 🚀 Quick Start

### Starting the Database Server

```bash
# Run the database server (listens on port 5050)
sdb
```

### Using the CLI Client

```bash
# Put a key-value pair
sdb-cli put mykey "Hello, World!"

# Get a value by key
sdb-cli get mykey

# Delete a key
sdb-cli delete mykey
```

### Using the Go Client

```go
package main

import (
    "fmt"
    "github.com/MartinSimango/simple-db/pkg/db"
)

func main() {
    client, err := db.NewSimpleDbClient(":5050")
    if err != nil {
        panic(err)
    }
    defer client.Close()

    // Put operation
    result, err := client.Put("mykey", "myvalue")
    if err != nil {
        panic(err)
    }
    fmt.Println("Put result:", result)

    // Get operation
    value, err := client.Get("mykey")
    if err != nil {
        panic(err)
    }
    fmt.Println("Retrieved value:", value)
}
```

## ⚡ Performance Testing

Simple-DB includes built-in load testing tools:

```bash
# Run concurrent load test
# Usage: sdb-test <connections> <requests_per_connection>
sdb-test 100 1000
```

## 📊 Technical Details

### Storage Engine

- **Memory Table**: Thread-safe in-memory data structure with pluggable implementations
- **WAL Format**: Protocol Buffers with record types (PUT, DELETE)
- **SSTable**: Block-based storage with 4KB blocks and 1MB max size
- **Record Limit**: Maximum 64KB per record

### Network Protocol

Simple-DB implements a lightweight, human-readable TCP protocol inspired by HTTP. Each request follows a structured format with clear delimiters, making it easy to debug and implement clients.

**Protocol Format:**
```
<OPERATION>\n
<HEADER_NAME>: <HEADER_VALUE>\n
[additional headers...]
\n
[body data for PUT operations]
```

**Protocol Components:**

1. **Operation Line**: First line specifies the database operation
   - `PUT` - Store or update a key-value pair
   - `GET` - Retrieve value for a given key
   - `DELETE` - Remove a key-value pair

2. **Headers Section**: Key-value metadata (colon-separated)
   - `Key: <string>` - The database key (required for all operations)
   - `Length: <integer>` - Byte length of the value (required for PUT operations)

3. **Empty Line**: Single newline `\n` separating headers from body

4. **Body**: Raw value data (only for PUT operations)

**Request Examples:**

**PUT Operation:**
```
PUT
Key: user:123
Length: 25

{"name": "John", "age": 30}
```

**GET Operation:**
```
GET
Key: user:123

```

**DELETE Operation:**
```
DELETE
Key: user:123

```

**Response Format:**
The server responds with simple status messages:
- `OK` - Operation successful
- `NOT_FOUND` - Key doesn't exist (GET/DELETE)
- `ERROR: <message>` - Operation failed

This protocol design prioritizes simplicity and debuggability over performance, making it ideal for educational purposes and easy client implementation.

### Concurrency

- Thread-safe operations using Go mutexes
- Concurrent client handling with goroutines
- Atomic operations for server state management


## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

