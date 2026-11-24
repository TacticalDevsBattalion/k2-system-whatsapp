package owner

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/TacticalDevsBattalion/k2-system-whatsapp/internal/dbretry"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

var fast = Config{OutageMax: 200 * time.Millisecond, Backoff: dbretry.Backoff{Initial: time.Millisecond, Max: 5 * time.Millisecond}}

type fakeLock struct {
	lost     chan struct{}
	cause    error
	released chan struct{}
	once     sync.Once
}

func newLock() *fakeLock { return &fakeLock{lost: make(chan struct{}), released: make(chan struct{})} }

func (l *fakeLock) Lost() <-chan struct{} { return l.lost }
func (l *fakeLock) Cause() error          { return l.cause }
func (l *fakeLock) Release()              { l.once.Do(func() { close(l.released) }) }
func (l *fakeLock) lose(err error)        { l.cause = err; close(l.lost) }

// world records what Run does and fails the test if owned work could overlap a lock attempt.
type world struct {
	t       *testing.T
	mu      sync.Mutex
	running bool // session started and not yet stopped
	held    *fakeLock
	starts  int
	stops   int
	owner   []bool
	// script: what each Locker call returns, in order; past the end the call blocks until ctx ends.
	script   []func() (*fakeLock, error)
	calls    int
	acquired chan *fakeLock
	startErr []error
}

func newWorld(t *testing.T) *world { return &world{t: t, acquired: make(chan *fakeLock, 16)} }

func (w *world) lock(ctx context.Context, onWait func()) (Lock, error) {
	w.mu.Lock()
	if w.running {
		w.t.Error("lock taken while the owned work still runs (split brain)")
	}
	if w.held != nil {
		select {
		case <-w.held.released:
		default:
			w.t.Error("lock taken again before the previous one was released")
		}
	}
	i := w.calls
	w.calls++
	w.mu.Unlock()
	if i >= len(w.script) {
		onWait()
		<-ctx.Done()
		return nil, ctx.Err()
	}
	l, err := w.script[i]()
	if err != nil {
		return nil, err
	}
	w.mu.Lock()
	w.held = l
	w.mu.Unlock()
	w.acquired <- l
	return l, nil
}

func (w *world) Start(ctx context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.starts++
	if len(w.startErr) > 0 {
		err := w.startErr[0]
		w.startErr = w.startErr[1:]
		if err != nil {
			return err
		}
	}
	w.running = true
	return nil
}

func (w *world) Stop() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.stops++
	w.running = false
}

func (w *world) setOwner(v bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if v && !w.running {
		w.t.Error("owner reported before the work runs")
	}
	w.owner = append(w.owner, v)
}

func ok() (*fakeLock, error) { return newLock(), nil }

func refused() (*fakeLock, error) {
	return nil, fmt.Errorf("lock connection: failed to connect to `user=wa_reader database=whatsapp`: %w",
		&net.OpError{Op: "dial", Net: "tcp", Err: os.NewSyscallError("connect", syscall.ECONNREFUSED)})
}

func run(w *world, cfg Config) (context.CancelFunc, chan error) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Run(ctx, cfg, w.lock, w, w.setOwner, quiet) }()
	return cancel, done
}

func waitLock(t *testing.T, w *world) *fakeLock {
	t.Helper()
	select {
	case l := <-w.acquired:
		return l
	case <-time.After(5 * time.Second):
		t.Fatal("lock never taken")
		return nil
	}
}

func waitOwner(t *testing.T, w *world, n int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		w.mu.Lock()
		got := len(w.owner)
		w.mu.Unlock()
		if got >= n {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("owner changes: want %d", n)
}

// The 2026-11-24 case: the lock's connection dies with a primary restart; the session pauses,
// Postgres refuses a few times, then the lock is taken again and the session resumes — no exit.
func TestLostLockPausesAndResumes(t *testing.T) {
	w := newWorld(t)
	w.script = []func() (*fakeLock, error){ok, refused, refused, refused, ok}
	cancel, done := run(w, fast)
	first := waitLock(t, w)
	waitOwner(t, w, 1)

	first.lose(errors.New("lock check: unexpected EOF"))
	second := waitLock(t, w)
	waitOwner(t, w, 3) // true, false, true
	select {
	case <-first.released:
	default:
		t.Fatal("lost lock not released")
	}

	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}
	<-second.released
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.starts != 2 || w.stops != 2 || w.running {
		t.Fatalf("starts=%d stops=%d running=%v", w.starts, w.stops, w.running)
	}
	want := []bool{true, false, true, false}
	if len(w.owner) != len(want) {
		t.Fatalf("owner = %v, want %v", w.owner, want)
	}
	for i := range want {
		if w.owner[i] != want[i] {
			t.Fatalf("owner = %v, want %v", w.owner, want)
		}
	}
}

func TestOutageLongerThanLimitEndsRun(t *testing.T) {
	w := newWorld(t)
	w.script = []func() (*fakeLock, error){ok}
	for i := 0; i < 10000; i++ {
		w.script = append(w.script, func() (*fakeLock, error) { return nil, syscall.ECONNREFUSED })
	}
	_, done := run(w, Config{OutageMax: 50 * time.Millisecond, Backoff: fast.Backoff})
	l := waitLock(t, w)
	waitOwner(t, w, 1)
	start := time.Now()
	l.lose(errors.New("lock connection closed"))
	select {
	case err := <-done:
		if !errors.Is(err, syscall.ECONNREFUSED) {
			t.Fatalf("err = %v", err)
		}
		if el := time.Since(start); el < 50*time.Millisecond {
			t.Fatalf("gave up after %s, before the limit", el)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not give up")
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.running || w.owner[len(w.owner)-1] {
		t.Fatal("session left running")
	}
}

func TestFatalErrorEndsRunAtOnce(t *testing.T) {
	w := newWorld(t)
	w.script = []func() (*fakeLock, error){func() (*fakeLock, error) {
		return nil, &pgconn.PgError{Code: "28P01", Severity: "FATAL"}
	}}
	_, done := run(w, fast)
	select {
	case err := <-done:
		var pe *pgconn.PgError
		if !errors.As(err, &pe) || pe.Code != "28P01" {
			t.Fatalf("err = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run kept retrying a bad password")
	}
	if w.calls != 1 {
		t.Fatalf("calls = %d", w.calls)
	}
}

// A session start that fails because Postgres went away again releases the lock and retries.
func TestStartFailureRetried(t *testing.T) {
	w := newWorld(t)
	w.script = []func() (*fakeLock, error){ok, ok}
	w.startErr = []error{&pgconn.PgError{Code: "57P01"}, nil}
	cancel, done := run(w, fast)
	l1 := waitLock(t, w)
	<-l1.released
	waitLock(t, w)
	waitOwner(t, w, 1)
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if w.starts != 2 || w.stops != 2 {
		t.Fatalf("starts=%d stops=%d", w.starts, w.stops)
	}
}

// Standing by behind another owner is not an outage, however long it lasts.
func TestStandbyIsNotAnOutage(t *testing.T) {
	w := newWorld(t)
	cancel, done := run(w, Config{OutageMax: time.Millisecond, Backoff: fast.Backoff})
	time.Sleep(30 * time.Millisecond)
	select {
	case err := <-done:
		t.Fatalf("standby ended Run: %v", err)
	default:
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
