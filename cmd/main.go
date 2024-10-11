package main

import (
    "bitcask-kvstore/pkg/store"
    "fmt"
    "os"
    "strconv"
    "time"
)

func main() {
    // Load existing data
    err := store.Load()
    if err != nil {
        fmt.Println("Error loading data:", err)
    }

    // Start expiry cleanup
    store.StartExpiryCleanup()

    if len(os.Args) < 2 {
        fmt.Println("Expected 'GET', 'PUT', 'DELETE', 'LIST', 'SYNC', 'CLOSE', or 'COMPACT' subcommands")
        os.Exit(1)
    }

    switch os.Args[1] {
    case "PUT":
        if len(os.Args) < 4 {
            fmt.Println("Usage: PUT <key> <value> [expiry in seconds]")
            os.Exit(1)
        }
        key := os.Args[2]
        value := os.Args[3]
        var duration time.Duration
        if len(os.Args) == 5 {
            seconds, err := strconv.Atoi(os.Args[4])
            if err != nil {
                fmt.Println("Invalid expiry duration")
                os.Exit(1)
            }
            duration = time.Duration(seconds) * time.Second
        }
        store.Put(key, value, duration)
        fmt.Println("Key added")

    case "GET":
        if len(os.Args) != 3 {
            fmt.Println("Usage: GET <key>")
            os.Exit(1)
        }
        value, err := store.Get(os.Args[2])
        if err != nil {
            fmt.Println(err)
        } else {
            fmt.Println("Value:", value)
        }

    case "DELETE":
        if len(os.Args) != 3 {
            fmt.Println("Usage: DELETE <key>")
            os.Exit(1)
        }
        store.Delete(os.Args[2])
        fmt.Println("Key deleted")

    case "LIST":
        keys := store.ListKeys()
        if len(keys) == 0 {
            fmt.Println("No keys found")
        } else {
            fmt.Println("Keys:", keys)
        }

    case "SYNC":
        err := store.Sync()
        if err != nil {
            fmt.Println("Error during sync:", err)
        } else {
            fmt.Println("Sync successful")
        }

    case "CLOSE":
        err := store.Close()
        if err != nil {
            fmt.Println("Error during close:", err)
        } else {
            fmt.Println("Datastore closed")
        }

    case "COMPACT":
        err := store.Compact()
        if err != nil {
            fmt.Println("Error during compaction:", err)
        } else {
            fmt.Println("Compaction completed")
        }

    default:
        fmt.Println("Unknown command:", os.Args[1])
    }
}
