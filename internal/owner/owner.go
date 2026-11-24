// Package owner keeps this process the single owner of the WhatsApp session across Postgres
// outages.
//
// Ownership is a session advisory lock on one dedicated connection. When that connection dies —
// a CNPG primary restart or switchover, a network cut — the lock is gone with it, and another
// replica may take it at once. So the owned work (WhatsApp, the outbox pump, pruning) is stopped
// first, and only then is the lock taken again on a fresh connection, retrying with backoff while
// Postgres is away. Until it is held again nothing is sent or published: two owners never run at
// once. Only an outage longer than Config.OutageMax ends the process (Run returns an error) so
// that Kubernetes restarts it.
package owner

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/TacticalDevsBattalion/k2-system-whatsapp/internal/dbretry"
)

// Lock is a held owner lock.
type Lock interface {
	// Lost is closed when the lock is no longer held (its connection died).
	Lost() <-chan struct{}
	// Cause says why it was lost; nil before that.
	Cause() error
	Release()
}

// Locker blocks until the lock is taken or fails. onWait is called (in Locker's goroutine) when
// the lock is busy — Postgres answered, another replica owns the session.
type Locker func(ctx context.Context, onWait func()) (Lock, error)

// Session is the work only the owner may do.
type Session interface {
	// Start begins the work under ctx, which is cancelled when ownership ends.
	Start(ctx context.Context) error
	// Stop returns once none of that work runs any more. It is called after every Start,
	// including one that failed.
	Stop()
}

type Config struct {
	// OutageMax is how long Postgres may stay unreachable before Run gives up.
	OutageMax time.Duration
	Backoff   dbretry.Backoff
}

// Run owns the session until ctx ends (it then stops the work, releases the lock and returns
// nil) or ownership cannot be regained within OutageMax, or a failure is not retryable.
// setOwner reports ownership (readiness) and is true only while the lock is held and the work runs.
func Run(ctx context.Context, cfg Config, lock Locker, s Session, setOwner func(bool), log *slog.Logger) error {
	var outageSince time.Time // zero while Postgres answers
	attempt := 0
	// fail decides what an error from taking the lock or starting the session means: retry after a
	// pause (true), or give up (false, with the error to return).
	fail := func(what string, err error) (bool, error) {
		if ctx.Err() != nil {
			return false, nil
		}
		if !dbretry.Retryable(err) {
			return false, fmt.Errorf("%s: %w", what, err)
		}
		if outageSince.IsZero() {
			outageSince = time.Now()
		}
		down := time.Since(outageSince)
		if down >= cfg.OutageMax {
			return false, fmt.Errorf("%s: postgres unavailable for %s (limit %s): %w",
				what, down.Round(time.Second), cfg.OutageMax, err)
		}
		delay := cfg.Backoff.Delay(attempt)
		attempt++
		log.Warn("Postgres unavailable; WhatsApp session paused, retrying", "step", what, "attempt", attempt,
			"in", delay.String(), "unavailableFor", down.Round(time.Second).String(), "err", err)
		return dbretry.Sleep(ctx, delay), nil
	}
	var lostAt time.Time
	for {
		waiting := false
		l, err := lock(ctx, func() {
			// Postgres answered: this is a standby, not an outage.
			outageSince, attempt = time.Time{}, 0
			if !waiting {
				waiting = true
				log.Info("Another replica owns the WhatsApp session; standing by")
			}
		})
		if err != nil {
			if again, ferr := fail("owner lock", err); !again {
				return ferr
			}
			continue
		}
		ownerCtx, endOwnership := context.WithCancel(ctx)
		if err := s.Start(ownerCtx); err != nil {
			endOwnership()
			s.Stop()
			l.Release()
			if again, ferr := fail("whatsapp start", err); !again {
				return ferr
			}
			continue
		}
		if lostAt.IsZero() {
			log.Info("Owner lock acquired")
		} else {
			log.Info("Owner lock re-acquired; WhatsApp session resumed", "pausedFor", time.Since(lostAt).Round(time.Millisecond).String())
		}
		outageSince, attempt, lostAt = time.Time{}, 0, time.Time{}
		setOwner(true)

		// Stopping order matters: readiness off, owned work cancelled and stopped, and only then
		// the lock released — so no other owner can start while ours is still running.
		stop := func() {
			setOwner(false)
			endOwnership()
			s.Stop()
			l.Release()
		}
		select {
		case <-ctx.Done():
			stop()
			return nil
		case <-l.Lost():
			lostAt = time.Now()
			outageSince = lostAt
			log.Error("Lost the owner lock; WhatsApp session paused until it is re-acquired", "err", l.Cause())
			stop()
		}
	}
}
