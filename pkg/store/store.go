package store

import (
    "bufio"
    "encoding/json"
    "errors"
    "fmt"
    "io"
    "os"
    "strconv"
    "strings"
    "time"
)

type Entry struct {
    Value     string
    ExpiresAt time.Time // Zero value means no expiry
}

var KeyDir = make(map[string]Entry)

const dataFile = "data.log"
const hintFile = "hintfile.log"

// StartExpiryCleanup starts a goroutine to remove expired keys
func StartExpiryCleanup() {
    ticker := time.NewTicker(1 * time.Minute)
    go func() {
        for {
            <-ticker.C
            now := time.Now()
            expiredKeys := false
            for key, entry := range KeyDir {
                if !entry.ExpiresAt.IsZero() && now.After(entry.ExpiresAt) {
                    Delete(key)
                    expiredKeys = true
                }
            }
            if expiredKeys {
                Compact()
            }
        }
    }()
}

// Load data from hint file
func Load() error {
    hintF, err := os.Open(hintFile)
    if err != nil {
        if os.IsNotExist(err) {
            return nil // No hint file, nothing to load
        }
        return err
    }
    defer hintF.Close()

    dataF, err := os.Open(dataFile)
    if err != nil {
        return err
    }
    defer dataF.Close()

    scanner := bufio.NewScanner(hintF)
    for scanner.Scan() {
        line := scanner.Text()
        parts := strings.Split(line, ":")
        if len(parts) == 3 {
            key := parts[0]
            offset, _ := strconv.ParseInt(parts[1], 10, 64)
            size, _ := strconv.Atoi(parts[2])

            dataF.Seek(offset, io.SeekStart)
            dataBytes := make([]byte, size)
            dataF.Read(dataBytes)

            var entry Entry
            json.Unmarshal(dataBytes, &entry)
            KeyDir[key] = entry
        } else if len(parts) == 2 && parts[1] == "DELETE" {
            delete(KeyDir, parts[0])
        }
    }
    return nil
}

// SaveEntry writes the entry to the data file and updates the hint file
func SaveEntry(key string, entry Entry) error {
    // Write to data file
    dataF, err := os.OpenFile(dataFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
    if err != nil {
        return err
    }
    defer dataF.Close()

    offset, err := dataF.Seek(0, io.SeekEnd)
    if err != nil {
        return err
    }

    dataBytes, err := json.Marshal(entry)
    if err != nil {
        return err
    }

    _, err = dataF.Write(append(dataBytes, '\n'))
    if err != nil {
        return err
    }

    // Write to hint file
    hintF, err := os.OpenFile(hintFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
    if err != nil {
        return err
    }
    defer hintF.Close()

    hintEntry := fmt.Sprintf("%s:%d:%d\n", key, offset, len(dataBytes))
    _, err = hintF.WriteString(hintEntry)
    if err != nil {
        return err
    }

    return nil
}

func Get(key string) (string, error) {
    err := Load() // Load the data before retrieving the key
    if err != nil {
        return "", err
    }

    entry, ok := KeyDir[key] // Get the entry from the map
    if ok {
        // Check if the entry has expired
        if !entry.ExpiresAt.IsZero() && time.Now().After(entry.ExpiresAt) {
            Delete(key) // Delete expired key
            return "", errors.New("key not found (expired)")
        }
        return entry.Value, nil // Return the value
    }
    return "", errors.New("key not found")
}

func Put(key, value string, duration time.Duration) {
    var expiresAt time.Time
    if duration > 0 {
        expiresAt = time.Now().Add(duration)
    }
    entry := Entry{Value: value, ExpiresAt: expiresAt}
    KeyDir[key] = entry

    err := SaveEntry(key, entry)
    if err != nil {
        fmt.Println("Error saving entry:", err)
    }
    fmt.Printf("Key-value pair added: %s = %s\n", key, value)
}

func Delete(key string) {
    delete(KeyDir, key)
    // Append delete marker to hint file
    hintF, err := os.OpenFile(hintFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
    if err != nil {
        fmt.Println("Error updating hint file:", err)
        return
    }
    defer hintF.Close()

    hintEntry := fmt.Sprintf("%s:DELETE\n", key)
    _, err = hintF.WriteString(hintEntry)
    if err != nil {
        fmt.Println("Error writing to hint file:", err)
    }
    fmt.Printf("Key %s deleted\n", key)
}

func ListKeys() []string {
    keys := make([]string, 0, len(KeyDir))
    for key := range KeyDir {
        keys = append(keys, key)
    }
    return keys
}

func Sync() error {
    // Force file system to sync data to disk
    fmt.Println("Data synced to disk")
    return nil
}

func Close() error {
    err := Sync()
    if err != nil {
        return err
    }
    fmt.Println("Datastore closed successfully")
    return nil
}

func Compact() error {
    tempDataFile := "data_temp.log"
    tempHintFile := "hint_temp.log"

    tempDataF, err := os.Create(tempDataFile)
    if err != nil {
        return err
    }
    defer tempDataF.Close()

    tempHintF, err := os.Create(tempHintFile)
    if err != nil {
        return err
    }
    defer tempHintF.Close()

    for key, entry := range KeyDir {
        // Skip expired entries
        if !entry.ExpiresAt.IsZero() && time.Now().After(entry.ExpiresAt) {
            continue
        }

        dataBytes, err := json.Marshal(entry)
        if err != nil {
            return err
        }

        offset, err := tempDataF.Seek(0, io.SeekEnd)
        if err != nil {
            return err
        }

        _, err = tempDataF.Write(append(dataBytes, '\n'))
        if err != nil {
            return err
        }

        hintEntry := fmt.Sprintf("%s:%d:%d\n", key, offset, len(dataBytes))
        _, err = tempHintF.WriteString(hintEntry)
        if err != nil {
            return err
        }
    }

    tempDataF.Sync()
    tempHintF.Sync()

    // Replace old files
    os.Remove(dataFile)
    os.Remove(hintFile)
    os.Rename(tempDataFile, dataFile)
    os.Rename(tempHintFile, hintFile)

    fmt.Println("Compaction completed successfully")
    return nil
}
