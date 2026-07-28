# tasker

A lightweight, thread-safe, in-memory background task runner for Go using generics. It provides built-in TTL cleanup, panic recovery, and seamless support for both **Short-Polling** and **Long-Polling** HTTP patterns.

## Features

* **Generic Task Runner:** Type-safe execution of background functions returning `(T, error)`.
* **Short & Long Polling:** Non-blocking status snapshots or blocking `Wait(ctx)` with context cancellation support.
* **Automatic TTL Cleanup:** Finished operations are automatically removed from memory after a specified Time-To-Live.
* **Panic Recovery:** Protects background goroutines from crashing your application by capturing panics and marking the operation as `failed`.
* **Thread-Safe:** Safe for concurrent access across multiple HTTP handlers.
* **Zero External Overhead:** In-memory execution without requiring Redis, RabbitMQ, or databases.

---

## Installation

```bash
go get github.com/rnmz/tasker
```

---

## Quick Start

### Basic Usage

```go
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/rnmz/tasker"
)

type ReportResult struct {
	ItemsProcessed int    `json:"items_processed"`
	DownloadURL    string `json:"download_url"`
}

func main() {
	// Create a manager with a 15-minute TTL for completed tasks
	mgr := tasker.NewManager(15 * time.Minute)

	// 1. Create a new operation
	op := mgr.Create()
	fmt.Println("Created operation ID:", op.ID)

	// 2. Run background task
	tasker.Run(mgr, op, func() (ReportResult, error) {
		time.Sleep(2 * time.Second) // Simulate heavy work
		return ReportResult{
			ItemsProcessed: 100,
			DownloadURL:    "https://example.com/report.pdf",
		}, nil
	})

	// 3. Wait for completion (blocking with timeout context)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	op.Wait(ctx)

	// 4. Retrieve execution snapshot
	status, result, errMsg := op.Snapshot()
	fmt.Printf("Status: %s, Result: %+v, Error: %s\n", status, result, errMsg)
}

```

---

## HTTP Integration Example

`tasker` excels at handling long-running background tasks (e.g., payment terminal processing, report generation, external API calls) in HTTP services.

```go
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/rnmz/tasker"
)

type Server struct {
	manager *tasker.Manager
}

func main() {
	srv := &Server{
		// Operations will stay in memory for 30 minutes after completion
		manager: tasker.NewManager(30 * time.Minute),
	}

	http.HandleFunc("POST /tasks", srv.handleCreateTask)
	http.HandleFunc("GET /tasks/", srv.handleGetTask)

	log.Println("Server running on :8080...")
	log.Fatal(http.ListenAndServe(":8080", nil))
}

// POST /tasks - Trigger background task
func (s *Server) handleCreateTask(w http.ResponseWriter, r *http.Request) {
	op := s.manager.Create()

	// Launch task asynchronously
	tasker.Run(s.manager, op, func() (any, error) {
		time.Sleep(10 * time.Second) // Work simulation

		return map[string]any{
			"message": "Report generated successfully",
			"count":   42,
		}, nil
	})

	// Return immediate 202 Accepted response
	respondJSON(w, http.StatusAccepted, map[string]any{
		"operation_id": op.ID,
		"status":       tasker.StatusPending,
	})
}

// GET /tasks/{id} - Status retrieval with Long-Polling support
func (s *Server) handleGetTask(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/tasks/")
	
	op, ok := s.manager.Get(id)
	if !ok {
		http.Error(w, "Task not found or TTL expired", http.StatusNotFound)
		return
	}

	// Enable Long-Polling if requested by client (?wait=true)
	if r.URL.Query().Get("wait") == "true" {
		ctx, cancel := context.WithTimeout(r.Context(), 25*time.Second)
		defer cancel()

		op.Wait(ctx) // Blocks until task completes or 25s timeout elapses
	}

	status, result, errMsg := op.Snapshot()
	response := map[string]any{
		"id":     op.ID,
		"status": status,
	}

	switch status {
	case tasker.StatusCompleted:
		response["result"] = result
	case tasker.StatusFailed:
		response["error"] = errMsg
	}

	respondJSON(w, http.StatusOK, response)
}

func respondJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(data)
}

```

---

## Polling Patterns Supported

### Short-Polling

Clients make periodic calls to `GET /tasks/{id}`. `Snapshot()` returns immediately without blocking.

```http
GET /tasks/3790ec38-c64c-4e89-a292-1b15cfae1ed2 HTTP/1.1

```

**Response (Instant):**

```json
{
  "id": "3790ec38-c64c-4e89-a292-1b15cfae1ed2",
  "status": "pending"
}

```

### Long-Polling

Clients add `?wait=true`. The server holds the request via `op.Wait(ctx)` and responds instantly as soon as the background goroutine finishes.

```http
GET /tasks/3790ec38-c64c-4e89-a292-1b15cfae1ed2?wait=true HTTP/1.1

```

**Response (When finished or timed out):**

```json
{
  "id": "3790ec38-c64c-4e89-a292-1b15cfae1ed2",
  "status": "completed",
  "result": {
    "count": 42,
    "message": "Report generated successfully"
  }
}

```

---

## License

[MIT](https://github.com/rnmz/tasker/blob/main/LICENSE)