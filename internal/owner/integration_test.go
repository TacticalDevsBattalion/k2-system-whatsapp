//go:build integration

// Lock loss against a real Postgres: the owner lock's backend is terminated (what a CNPG primary
// restart does to it), and Run must pause the session, take the lock again and resume.
//
//	WA_TEST_DATABASE_URL=postgres://wa_reader:…@localhost:5432/whatsapp go test -tags integration ./internal/owner/
//
// A whole-server restart and a longer outage (docker restart / stop + start of Postgres) were
// checked by hand the same way; CI cannot restart its service container.
package owner

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/TacticalDevsBattalion/k2-system-whatsapp/internal/dbretry"
	"github.com/TacticalDevsBattalion/k2-system-whatsapp/internal/store"
)

type recSession struct {
	mu      sync.Mutex
	running bool
	starts  int
	started chan struct{}
}

func (s *recSession) Start(context.Context) error {
	s.mu.Lock()
	s.running = true
	s.starts++
	s.mu.Unlock()
	s.started <- struct{}{}
	return nil
}

func (s *recSession) Stop() {
	s.mu.Lock()
	s.running = false
	s.mu.Unlock()
}

func TestLockLossAgainstPostgres(t *testing.T) {
	dsn := os.Getenv("WA_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("WA_TEST_DATABASE_URL not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	st, err := store.Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	const clientID = "owner-loss-it"
	sess := &recSession{started: make(chan struct{}, 4)}
	var ownerMu sync.Mutex
	var owner bool
	setOwner := func(v bool) { ownerMu.Lock(); owner = v; ownerMu.Unlock() }
	runCtx, stopRun := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		done <- Run(runCtx, Config{OutageMax: 2 * time.Minute, Backoff: dbretry.Backoff{Initial: 200 * time.Millisecond, Max: 2 * time.Second}},
			func(ctx context.Context, onWait func()) (Lock, error) {
				l, err := st.AcquireOwnerLock(ctx, clientID, onWait)
				if err != nil {
					return nil, err
				}
				return l, nil
			}, sess, setOwner, slog.New(slog.NewTextHandler(tWriter{t}, nil)))
	}()
	waitStart := func(what string) {
		t.Helper()
		select {
		case <-sess.started:
		case err := <-done:
			t.Fatalf("%s: Run ended: %v", what, err)
		case <-time.After(90 * time.Second):
			t.Fatalf("%s: session not started", what)
		}
	}
	waitStart("first term")

	// Exclusive while held: a contender only stands by.
	cctx, ccancel := context.WithTimeout(ctx, 2*time.Second)
	if _, err := st.AcquireOwnerLock(cctx, clientID, nil); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("contender got the lock: %v", err)
	}
	ccancel()

	// Kill the lock's backend, as a primary restart does.
	var killed bool
	if err := st.Pool.QueryRow(ctx, `select coalesce(bool_or(pg_terminate_backend(pid)), false) from pg_locks
		where locktype = 'advisory' and granted and objsubid = 1
		and ((classid::bigint << 32) | objid::bigint) = $1`, store.LockKey(clientID)).Scan(&killed); err != nil || !killed {
		t.Fatalf("terminate lock backend: killed=%v err=%v", killed, err)
	}
	waitStart("after the lock backend was terminated")

	ownerMu.Lock()
	if !owner {
		t.Error("not owner after re-acquiring")
	}
	ownerMu.Unlock()
	stopRun()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}
	sess.mu.Lock()
	defer sess.mu.Unlock()
	if sess.running {
		t.Fatal("session left running")
	}
	// Released: the lock is free again.
	lctx, lcancel := context.WithTimeout(ctx, 10*time.Second)
	defer lcancel()
	l, err := st.AcquireOwnerLock(lctx, clientID, nil)
	if err != nil {
		t.Fatalf("lock not free after Run: %v", err)
	}
	l.Release()
}

type tWriter struct{ t *testing.T }

func (w tWriter) Write(p []byte) (int, error) { w.t.Log(string(p)); return len(p), nil }
