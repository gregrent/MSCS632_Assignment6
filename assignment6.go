package main

import (
    "bufio"
    "fmt"
    "log"
    "math/rand"
    "os"
    "sync"
    "time"
)

type Task string
type Result string

func worker(id int, tasks <-chan Task, results chan<- Result, wg *sync.WaitGroup) {
    defer wg.Done()
    log.Printf("Worker-%d started.\n", id)
    for t := range tasks {
        func(task Task) {
            defer func() {
                if r := recover(); r != nil {
                    log.Printf("Worker-%d panic recovered: %v\n", id, r)
                }
            }()
            // simulate processing
            time.Sleep(time.Duration(500+rand.Intn(500)) * time.Millisecond)
            res := Result(fmt.Sprintf("Worker-%d processed: %s", id, task))
            results <- res
            log.Println(res)
        }(t)
    }
    log.Printf("Worker-%d exiting.\n", id)
}

func main() {
    rand.Seed(time.Now().UnixNano())
    logFile, err := os.OpenFile("rideshare_go.log", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
    if err != nil {
        log.Fatalf("Cannot open log file: %v", err)
    }
    defer logFile.Close()
    log.SetOutput(logFile)

    taskChan := make(chan Task)
    resultChan := make(chan Result)
    var wg sync.WaitGroup

    numWorkers := 5
    wg.Add(numWorkers)
    for i := 1; i <= numWorkers; i++ {
        go worker(i, taskChan, resultChan, &wg)
    }

    // gather results
    go func() {
        f, err := os.Create("output_go.txt")
        if err != nil {
            log.Fatalf("Cannot create output file: %v", err)
        }
        defer f.Close()
        writer := bufio.NewWriter(f)
        for r := range resultChan {
            if _, err := writer.WriteString(string(r) + "\n"); err != nil {
                log.Printf("Error writing result: %v\n", err)
            }
        }
        writer.Flush()
    }()

    // enqueue tasks
    for i := 1; i <= 20; i++ {
        task := Task(fmt.Sprintf("RideRequest#%d", i))
        taskChan <- task
    }

    close(taskChan)  // signal workers no more tasks
    wg.Wait()
    close(resultChan) // signal result writer
    log.Println("Processing done.")
}