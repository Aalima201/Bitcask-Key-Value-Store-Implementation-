# Bitcask-Key-Value-Store-Implementation-


## Introduction
The Bitcask key-value store is an efficient embedded database designed for production-grade traffic. Leveraging a write-once, append-only on-disk data format and an in-memory hash table for lookups, Bitcask excels in performance and reliability.

## Problem Statement
This implementation covers the basic features of the Bitcask key-value store, including:

- **GET**: Retrieve a value by key.
- **PUT**: Store a key and value.
- **DELETE**: Remove a key.
- **LIST KEYS**: List all stored keys.
- **SYNC**: Force writes to sync to disk.
- **CLOSE**: Close the datastore and flush pending writes.

### Additional features
- **EXPIRY**: Set an expiry time when adding a key (e.g., `PUT hello world 10s` sets the key "hello" to expire in 10 seconds).
- **Hint Files**: Create a hint file containing keys and metadata for faster boot times after a crash, facilitating the rebuilding of the KeyDir.
- **Compaction**: Merge older data files into new ones containing only active keys, optimizing disk space and improving performance based on thresholds for file size and disk usage.

## Features
- Efficient storage and retrieval of key-value pairs.
- Optional expiry for stored keys.
- Fast boot-up process with hint files.
- Automatic compaction to manage disk usage.

## Installation
1. Install Go if not already installed. Refer to [Go installation](https://golang.org/doc/install) for instructions.

## Usage
To use the database, run the `main.go` file and execute commands in the terminal:

### PUT: Store a value
```bash
go run cmd/main.go <COMMAND> <KEY> <VALUE>
```
### PUT: Store a value that has an expiry time
```bash
go run cmd/main.go PUT hello world 10s
```
### GET: Retrieve a value
```bash
go run cmd/main.go GET key1
```
### DELETE: Remove a key
```bash
go run cmd/main.go DELETE key1
```
### LIST KEYS: List all keys
```bash
go run cmd/main.go LIST KEYS
```
### SYNC: Sync writes to disk
```bash
go run cmd/main.go SYNC
```
### CLOSE: Close the datastore
```bash
go run cmd/main.go CLOSE
```
### COMPACTION: Optimize data storage
```bash
go run cmd/main.go COMPACTION
```
