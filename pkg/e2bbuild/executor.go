// Package e2bbuild runs template builds in this process: claim a sandbox from
// a pool, promote it under the build's template name, let the caller publish
// the result, and release the claim. Build records live in memory, so a build
// is served only by the replica that started it.
package e2bbuild

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/projecteru2/core/log"

	"github.com/cocoonstack/sandbox-operator/pkg/scale"
)

const (
	StatusWaiting  = "waiting"
	StatusBuilding = "building"
	StatusReady    = "ready"
	StatusError    = "error"

	// PhaseClaim, PhasePromote and PhasePublish are the phases a log line and a failure name.
	PhaseClaim   = "claim"
	PhasePromote = "promote"
	PhasePublish = "publish"

	recordTTL = time.Hour
)

var (
	// ErrBusy refuses a start while every build slot runs; the build stays waiting and a retry starts it.
	ErrBusy = errors.New("e2bbuild: every build slot is busy")
	// ErrUnknownBuild is a build this process never registered, or dropped an hour after it finished or was left unstarted.
	ErrUnknownBuild = errors.New("e2bbuild: build not found")
)

// Request is what a build asked for before it starts.
type Request struct {
	Size string
	Tags []string
}

// Spec is how a registered build runs; Publish runs after the promote and before the build reads ready.
type Spec struct {
	Namespace  string
	ClaimName  string
	Pool       scale.PoolKey
	Template   string
	TTLSeconds int
	Publish    func(ctx context.Context, node string, key scale.PoolKey, digest string) error
}

// LogEntry is one line of a build's log.
type LogEntry struct {
	Time    time.Time
	Level   string
	Message string
	Phase   string
}

// Info is a build's state as a status poll reads it.
type Info struct {
	Status      string
	Logs        []LogEntry
	FailedPhase string
	Failure     string
}

type record struct {
	req        Request
	info       Info
	registered time.Time
	finished   time.Time
}

// Executor runs at most its slot count of builds at once.
type Executor struct {
	store    scale.SandboxStore
	slots    chan struct{}
	timeout  time.Duration
	logLines int

	mu     sync.Mutex
	builds map[string]*record
}

// New builds an Executor over store with parallel slots, a per-build timeout and a log cap.
func New(store scale.SandboxStore, parallel int, timeout time.Duration, logLines int) *Executor {
	return &Executor{
		store: store, slots: make(chan struct{}, parallel), timeout: timeout, logLines: logLines,
		builds: map[string]*record{},
	}
}

// Register records a waiting build under id.
func (e *Executor) Register(id string, req Request) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.sweep(time.Now())
	e.builds[id] = &record{req: req, info: Info{Status: StatusWaiting}, registered: time.Now()}
}

// Request returns what a registered build asked for.
func (e *Executor) Request(id string) (Request, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	r, ok := e.builds[id]
	if !ok {
		return Request{}, false
	}
	return r.req, true
}

// Start runs a waiting build in the background; a build already started is left alone, so a retried start is a no-op.
func (e *Executor) Start(ctx context.Context, id string, spec Spec) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	r, ok := e.builds[id]
	if !ok {
		return ErrUnknownBuild
	}
	if r.info.Status != StatusWaiting {
		return nil
	}
	select {
	case e.slots <- struct{}{}:
	default:
		return ErrBusy
	}
	r.info.Status = StatusBuilding
	go e.run(context.WithoutCancel(ctx), id, spec)
	return nil
}

// Status returns a copy of the build's state.
func (e *Executor) Status(id string) (Info, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.sweep(time.Now())
	r, ok := e.builds[id]
	if !ok {
		return Info{}, false
	}
	info := r.info
	info.Logs = append([]LogEntry(nil), r.info.Logs...)
	return info, true
}

func (e *Executor) run(ctx context.Context, id string, spec Spec) {
	defer func() { <-e.slots }()
	ctx, cancel := context.WithTimeout(ctx, e.timeout)
	defer cancel()
	e.logf(id, PhaseClaim, "claiming a sandbox from %s (%s, %s)", spec.Pool.Template, spec.Pool.Net, spec.Pool.Size)
	a, err := e.store.Claim(ctx, spec.Namespace, spec.ClaimName, spec.Pool, scale.ClaimOptions{TTLSeconds: spec.TTLSeconds})
	if err != nil {
		msg := fmt.Sprintf("could not claim a sandbox of %s (%s, %s)", spec.Pool.Template, spec.Pool.Net, spec.Pool.Size)
		if scale.IsNoWarmCapacity(err) {
			msg = fmt.Sprintf("no warm sandbox of %s (%s, %s); a pool must serve the build's image and size", spec.Pool.Template, spec.Pool.Net, spec.Pool.Size)
		}
		e.fail(ctx, id, PhaseClaim, msg, err)
		return
	}
	e.logf(id, PhaseClaim, "claimed %s on %s", a.SandboxName, a.Node)
	phase, msg, err := e.promote(ctx, id, a, spec)
	e.release(ctx, a)
	if err != nil {
		e.fail(ctx, id, phase, msg, err)
		return
	}
	e.finish(id, StatusReady, "", "")
}

// promote returns the failed phase and the failure the caller sees next to the error the server logs.
func (e *Executor) promote(ctx context.Context, id string, a scale.Assignment, spec Spec) (string, string, error) {
	key, digest, err := e.store.Promote(ctx, a.Node, a.SandboxName, spec.Template)
	if err != nil {
		return PhasePromote, "could not promote the build's sandbox", err
	}
	e.logf(id, PhasePromote, "promoted as %s, content digest %s", key.Template, digest)
	if spec.Publish == nil {
		return "", "", nil
	}
	if err := spec.Publish(ctx, a.Node, key, digest); err != nil {
		return PhasePublish, "could not publish the build over the previous one", err
	}
	return "", "", nil
}

func (e *Executor) release(ctx context.Context, a scale.Assignment) {
	ctx = context.WithoutCancel(ctx)
	if err := e.store.Release(ctx, a.Node, a.SandboxName); err != nil {
		log.WithFunc("e2bbuild.release").Warnf(ctx, "release the build claim sandboxID=%s node=%s: %v", a.SandboxName, a.Node, err)
	}
}

func (e *Executor) fail(ctx context.Context, id, step, msg string, err error) {
	log.WithFunc("e2bbuild.fail").Errorf(ctx, err, "build %s failed at %s: %s", id, step, msg)
	e.log(id, step, "error", msg)
	e.finish(id, StatusError, step, msg)
}

func (e *Executor) finish(id, status, step, failure string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if r, ok := e.builds[id]; ok {
		r.info.Status, r.info.FailedPhase, r.info.Failure = status, step, failure
		r.finished = time.Now()
	}
}

func (e *Executor) logf(id, step, format string, args ...any) {
	e.log(id, step, "info", fmt.Sprintf(format, args...))
}

func (e *Executor) log(id, step, level, message string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	r, ok := e.builds[id]
	if !ok || len(r.info.Logs) >= e.logLines {
		return
	}
	r.info.Logs = append(r.info.Logs, LogEntry{Time: time.Now(), Level: level, Message: message, Phase: step})
}

func (e *Executor) sweep(now time.Time) {
	for id, r := range e.builds {
		done := !r.finished.IsZero() && now.Sub(r.finished) > recordTTL
		abandoned := r.info.Status == StatusWaiting && now.Sub(r.registered) > recordTTL
		if done || abandoned {
			delete(e.builds, id)
		}
	}
}
