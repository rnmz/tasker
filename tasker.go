package tasker

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
)

type Status string

const (
	StatusPending   Status = "pending"
	StatusCompleted Status = "completed"
	StatusFailed    Status = "failed"
)

type Operation struct {
	ID     string
	mu     sync.RWMutex
	status Status
	result any
	errMsg string
	done   chan struct{}
}

func newOperation(id string) *Operation {
	return &Operation{
		ID:     id,
		status: StatusPending,
		done:   make(chan struct{}),
	}
}

func (o *Operation) complete(status Status, result any, errMsg string) {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.status != StatusPending {
		return
	}
	o.status = status
	o.result = result
	o.errMsg = errMsg
	close(o.done)
}

func (o *Operation) Snapshot() (status Status, result any, errMsg string) {
	o.mu.RLock()
	defer o.mu.RUnlock()
	return o.status, o.result, o.errMsg
}

func (o *Operation) Wait(ctx context.Context) {
	select {
	case <-o.done:
	case <-ctx.Done():
	}
}

type Manager struct {
	mu  sync.RWMutex
	ops map[string]*Operation
	ttl time.Duration
}

func NewManager(ttl time.Duration) *Manager {
	return &Manager{
		ops: make(map[string]*Operation),
		ttl: ttl,
	}
}

func (m *Manager) Create() *Operation {
	op := newOperation(uuid.NewString())

	m.mu.Lock()
	m.ops[op.ID] = op
	m.mu.Unlock()

	return op
}

func (m *Manager) Get(id string) (*Operation, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	op, ok := m.ops[id]
	return op, ok
}

func Run[T any](m *Manager, op *Operation, fn func() (T, error)) {
	go func() {
		defer func() {
			if r := recover(); r != nil {
				op.complete(StatusFailed, nil, fmt.Sprintf("panic recovered: %v", r))
				m.scheduleCleanup(op.ID)
			}
		}()

		result, err := fn()
		if err != nil {
			op.complete(StatusFailed, nil, err.Error())
			m.scheduleCleanup(op.ID)
			return
		}

		op.complete(StatusCompleted, result, "")
		m.scheduleCleanup(op.ID)
	}()
}

func (m *Manager) scheduleCleanup(id string) {
	time.AfterFunc(m.ttl, func() {
		m.mu.Lock()
		delete(m.ops, id)
		m.mu.Unlock()
	})
}
