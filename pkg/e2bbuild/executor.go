// Package e2bbuild runs template builds in this process: claim a sandbox from
// a pool, run the build's steps and start command in it, promote it under the
// build's template name, let the caller publish the result, and release the
// claim. Build records live in memory, so a build is served only by the
// replica that started it.
package e2bbuild

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"path"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/projecteru2/core/log"

	"github.com/cocoonstack/sandbox-operator/pkg/sandboxd"
	"github.com/cocoonstack/sandbox-operator/pkg/scale"
)

const (
	StatusWaiting  = "waiting"
	StatusBuilding = "building"
	StatusReady    = "ready"
	StatusError    = "error"

	// PhaseClaim, PhaseFinalize, PhasePromote and PhasePublish are the phases a log line and a failure name; a step's phase is its 1-based index.
	PhaseClaim    = "claim"
	PhaseFinalize = "finalize"
	PhasePromote  = "promote"
	PhasePublish  = "publish"

	stepRun     = "RUN"
	stepEnv     = "ENV"
	stepWorkdir = "WORKDIR"
	stepUser    = "USER"
	stepCopy    = "COPY"

	defaultUser = "user"
	rootUser    = "root"
	readyPoll   = time.Second
	recordTTL   = time.Hour
)

var (
	// ErrBusy refuses a start while every build slot runs; the build stays waiting and a retry starts it.
	ErrBusy = errors.New("e2bbuild: every build slot is busy")
	// ErrUnknownBuild is a build this process never registered, or dropped an hour after it finished or was left unstarted.
	ErrUnknownBuild = errors.New("e2bbuild: build not found")

	// FilesHash is the shape of the digest the SDK names a COPY upload by.
	FilesHash = regexp.MustCompile(`^[0-9a-f]{64}$`)

	envEscaper = strings.NewReplacer(`\`, `\\`, `"`, `\"`, "`", "\\`", "$(", `\$(`)
)

// LineFunc receives one line of a command's output.
type LineFunc func(line string)

// ArchiveFunc opens the upload a COPY step's files hash names.
type ArchiveFunc func(ctx context.Context, hash string) (io.ReadCloser, error)

// PublishFunc runs after the promote with the template's node, key and content digest.
type PublishFunc func(ctx context.Context, node string, key scale.PoolKey, digest string) error

// Request is what a build asked for before it starts.
type Request struct {
	Size string
	Tags []string
}

// Step is one build instruction: RUN [command, user?], ENV [key, value, ...], WORKDIR [dir], USER [name] or COPY [src, dest, owner?, mode?] of the archive FilesHash names.
type Step struct {
	Type      string   `json:"type"`
	Args      []string `json:"args"`
	FilesHash string   `json:"filesHash,omitempty"`
}

func (s Step) problem(canCopy bool) string {
	switch s.Type {
	case stepRun, stepWorkdir, stepUser:
		if len(s.Args) == 0 || s.Args[0] == "" {
			return "needs an argument"
		}
	case stepEnv:
		if len(s.Args) == 0 || len(s.Args)%2 != 0 {
			return "needs key and value pairs"
		}
	case stepCopy:
		if !canCopy {
			return "no upload store is configured"
		}
		if len(s.Args) < 2 || s.Args[0] == "" || s.Args[1] == "" {
			return "needs a source and a destination"
		}
		if !FilesHash.MatchString(s.FilesHash) {
			return "needs the filesHash of its upload"
		}
	default:
		return "not a supported step type"
	}
	return ""
}

// Spec is how a registered build runs; StartCmd runs in the background and ReadyCmd until it exits 0, both before the promote; Publish runs after the promote and before the build reads ready.
type Spec struct {
	Namespace string
	ClaimName string
	Pool      scale.PoolKey
	Template  string
	Steps     []Step
	StartCmd  string
	ReadyCmd  string
	Archive   ArchiveFunc
	Publish   PublishFunc
}

// Command is one shell line run as User, in Workdir, with Envs; an empty Workdir is the user's home.
type Command struct {
	Line    string
	User    string
	Workdir string
	Envs    map[string]string
}

// Guest runs a build's commands inside its claimed sandbox.
type Guest interface {
	// Run runs cmd to its end, passes each line of its output to stdout or stderr, and returns its exit code.
	Run(ctx context.Context, a scale.Assignment, cmd Command, stdout, stderr LineFunc) (int, error)
	// Start starts cmd and returns while it runs.
	Start(ctx context.Context, a scale.Assignment, cmd Command) error
	// Init makes the user, workdir and envs of defaults what every later process in the sandbox gets.
	Init(ctx context.Context, a scale.Assignment, defaults Command) error
	// Write stores r at path in the sandbox as root.
	Write(ctx context.Context, a scale.Assignment, path string, r io.Reader) error
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
	guest    Guest
	slots    chan struct{}
	timeout  time.Duration
	logLines int

	mu     sync.Mutex
	builds map[string]*record
}

// New builds an Executor over store and guest with parallel slots, a per-build timeout and a log cap.
func New(store scale.SandboxStore, guest Guest, parallel int, timeout time.Duration, logLines int) *Executor {
	return &Executor{
		store: store, guest: guest, slots: make(chan struct{}, parallel), timeout: timeout, logLines: logLines,
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
	info.Logs = slices.Clip(r.info.Logs)
	return info, true
}

func (e *Executor) run(ctx context.Context, id string, spec Spec) {
	defer func() { <-e.slots }()
	ctx, cancel := context.WithTimeout(ctx, e.timeout)
	defer cancel()
	e.logf(id, PhaseClaim, "claiming a sandbox from %s (%s, %s)", spec.Pool.Template, spec.Pool.Net, spec.Pool.Size)
	a, err := e.store.Claim(ctx, spec.Namespace, spec.ClaimName, spec.Pool, scale.ClaimOptions{TTLSeconds: int(e.timeout / time.Second)})
	if err != nil {
		msg := fmt.Sprintf("could not claim a sandbox of %s (%s, %s)", spec.Pool.Template, spec.Pool.Net, spec.Pool.Size)
		if scale.IsNoWarmCapacity(err) {
			msg = fmt.Sprintf("no warm sandbox of %s (%s, %s); a pool must serve the build's image and size", spec.Pool.Template, spec.Pool.Net, spec.Pool.Size)
		}
		e.fail(ctx, id, PhaseClaim, msg, err)
		return
	}
	e.logf(id, PhaseClaim, "claimed %s on %s", a.SandboxName, a.Node)
	phase, msg, err := e.prepare(ctx, id, a, spec)
	if err == nil {
		phase, msg, err = e.promote(ctx, id, a, spec)
	}
	e.release(ctx, a)
	if err != nil {
		e.fail(ctx, id, phase, msg, err)
		return
	}
	e.finish(id, StatusReady, "", "")
}

func (e *Executor) prepare(ctx context.Context, id string, a scale.Assignment, spec Spec) (string, string, error) {
	state := Command{User: defaultUser, Envs: map[string]string{}}
	if a.NetRoute == sandboxd.NetRouteRelay {
		maps.Copy(state.Envs, sandboxd.RelayEnv)
	}
	for i, step := range spec.Steps {
		phase := strconv.Itoa(i + 1)
		next, msg, err := e.step(ctx, id, phase, a, spec, state, step)
		if err != nil {
			return phase, fmt.Sprintf("step %s (%s) failed: %s", phase, step.Type, msg), err
		}
		state = next
	}
	if err := e.guest.Init(ctx, a, state); err != nil {
		return PhaseFinalize, "could not set the template's user, workdir and environment", err
	}
	if spec.StartCmd != "" {
		e.logf(id, PhaseFinalize, "starting %s", spec.StartCmd)
		if err := e.guest.Start(ctx, a, withLine(state, spec.StartCmd)); err != nil {
			return PhaseFinalize, "could not start the start command", err
		}
	}
	if spec.ReadyCmd != "" {
		if err := e.awaitReady(ctx, a, withLine(state, spec.ReadyCmd)); err != nil {
			return PhaseFinalize, fmt.Sprintf("the ready command %q did not exit 0 before the build timed out", spec.ReadyCmd), err
		}
		e.logf(id, PhaseFinalize, "ready: %s exited 0", spec.ReadyCmd)
	}
	return "", "", nil
}

func (e *Executor) step(ctx context.Context, id, phase string, a scale.Assignment, spec Spec, state Command, s Step) (Command, string, error) {
	root := Command{User: rootUser, Envs: state.Envs}
	logged := func(line string) { e.log(id, phase, "info", line) }
	switch s.Type {
	case stepRun:
		cmd := withLine(state, s.Args[0])
		if len(s.Args) > 1 {
			cmd.User = s.Args[1]
		}
		msg, err := e.sh(ctx, a, cmd, logged, logged)
		return state, msg, err
	case stepEnv:
		envs := maps.Clone(state.Envs)
		for i := 0; i+1 < len(s.Args); i += 2 {
			v, msg, err := e.expand(ctx, a, root, s.Args[i+1], logged)
			if err != nil {
				return state, msg, err
			}
			envs[s.Args[i]] = v
		}
		state.Envs = envs
	case stepWorkdir:
		dir := s.Args[0]
		if !path.IsAbs(dir) {
			dir = path.Join(cmp.Or(state.Workdir, "/"), dir)
		}
		script := fmt.Sprintf(`t=%[1]s; [ -d "$t" ] && exit 0; n=$t; while [ ! -d "$(dirname "$n")" ]; do n=$(dirname "$n"); done; mkdir -p "$t" && chown -R %[2]s: "$n"`, shellQuote(dir), shellQuote(state.User))
		if msg, err := e.sh(ctx, a, withLine(root, script), logged, logged); err != nil {
			return state, msg, err
		}
		state.Workdir = dir
	case stepUser:
		name := shellQuote(s.Args[0])
		script := fmt.Sprintf("id -u %[1]s >/dev/null 2>&1 || useradd --create-home --shell /bin/bash %[1]s", name)
		if msg, err := e.sh(ctx, a, withLine(root, script), logged, logged); err != nil {
			return state, msg, err
		}
		state.User = s.Args[0]
	case stepCopy:
		if msg, err := e.copyIn(ctx, a, spec, s.FilesHash); err != nil {
			return state, msg, err
		}
		if _, err := e.sh(ctx, a, withLine(root, copyScript(state, s)), logged, logged); err != nil {
			return state, fmt.Sprintf("could not copy %s to %s", s.Args[0], s.Args[1]), err
		}
	}
	return state, "", nil
}

// copyIn writes the upload hash names to /tmp/<hash>.tar in the sandbox.
func (e *Executor) copyIn(ctx context.Context, a scale.Assignment, spec Spec, hash string) (string, error) {
	archive, err := spec.Archive(ctx, hash)
	if err != nil {
		return "could not read the uploaded files", err
	}
	defer func() { _ = archive.Close() }()
	if err := e.guest.Write(ctx, a, archivePath(hash), archive); err != nil {
		return "could not copy the uploaded files into the build sandbox", err
	}
	return "", nil
}

// sh returns the failure the caller sees next to the error; a non-zero exit is a failure.
func (e *Executor) sh(ctx context.Context, a scale.Assignment, cmd Command, stdout, stderr LineFunc) (string, error) {
	code, err := e.guest.Run(ctx, a, cmd, stdout, stderr)
	if err != nil {
		return "could not run a command in the build sandbox", err
	}
	if code != 0 {
		msg := fmt.Sprintf("%q exited with code %d", cmd.Line, code)
		return msg, errors.New(msg)
	}
	return "", nil
}

// expand evaluates an ENV value in the guest's shell, so $VAR references resolve and command substitution does not run; the shell's own stderr goes to the log, never into the value.
func (e *Executor) expand(ctx context.Context, a scale.Assignment, root Command, value string, stderr LineFunc) (string, string, error) {
	var lines []string
	if _, err := e.sh(ctx, a, withLine(root, `printf "%s" "`+envEscaper.Replace(value)+`"`), func(line string) { lines = append(lines, line) }, stderr); err != nil {
		return "", fmt.Sprintf("could not evaluate the value %q", value), err
	}
	return strings.Join(lines, "\n"), "", nil
}

func (e *Executor) awaitReady(ctx context.Context, a scale.Assignment, cmd Command) error {
	for {
		code, err := e.guest.Run(ctx, a, cmd, func(string) {}, func(string) {})
		if err == nil && code == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return errors.Join(ctx.Err(), err)
		case <-time.After(readyPoll):
		}
	}
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
	maps.DeleteFunc(e.builds, func(_ string, r *record) bool {
		done := !r.finished.IsZero() && now.Sub(r.finished) > recordTTL
		abandoned := r.info.Status == StatusWaiting && now.Sub(r.registered) > recordTTL
		return done || abandoned
	})
}

// Invalid names the first step a build cannot run, empty when every step can; a COPY needs canCopy.
func Invalid(steps []Step, canCopy bool) string {
	for i, s := range steps {
		if p := s.problem(canCopy); p != "" {
			return fmt.Sprintf("step %d (%s): %s", i+1, s.Type, p)
		}
	}
	return ""
}

func withLine(state Command, line string) Command {
	state.Line = line
	return state
}

func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}
