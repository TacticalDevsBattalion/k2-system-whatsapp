// Package dbretry tells a Postgres that is briefly away (a CNPG primary restart or switchover, a
// network blip, DNS settling) from one that will never let us in (wrong password, missing
// database, a certificate that does not verify), and retries only the former.
package dbretry

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

// Retryable reports whether err means "Postgres is not reachable right now" rather than "this
// will never work". Unknown errors are not retryable: a fault we cannot name should surface.
func Retryable(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) {
		return false
	}
	// Errors that retrying cannot fix come first: a joined error may carry both kinds.
	if fatalTLS(err) {
		return false
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return retryableSQLState(pgErr.Code)
	}
	if pgconn.Timeout(err) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	for _, e := range []error{syscall.ECONNREFUSED, syscall.ECONNRESET, syscall.ECONNABORTED, syscall.EPIPE,
		syscall.EHOSTUNREACH, syscall.ENETUNREACH, syscall.ETIMEDOUT, io.EOF, io.ErrUnexpectedEOF, net.ErrClosed} {
		if errors.Is(err, e) {
			return true
		}
	}
	var netErr net.Error // *net.OpError, *net.DNSError and timeouts
	return errors.As(err, &netErr)
}

// retryableSQLState: class 08 (connection exception) and 57P01–57P03 (admin shutdown, crash
// shutdown, cannot connect now — what a restarting or recovering server answers). Everything else
// from the server, 28P01/28000 (authentication) and 3D000 (no such database) among it, is final.
func retryableSQLState(code string) bool {
	switch code {
	case "57P01", "57P02", "57P03":
		return true
	}
	return strings.HasPrefix(code, "08")
}

func fatalTLS(err error) bool {
	var unknownCA x509.UnknownAuthorityError
	var hostname x509.HostnameError
	var invalid x509.CertificateInvalidError
	var verify *tls.CertificateVerificationError
	return errors.As(err, &unknownCA) || errors.As(err, &hostname) || errors.As(err, &invalid) || errors.As(err, &verify)
}

// Backoff is an exponential delay: Initial, doubled each attempt, never above Max.
type Backoff struct {
	Initial, Max time.Duration
}

// Delay is the pause after the attempt-th consecutive failure (0-based).
func (b Backoff) Delay(attempt int) time.Duration {
	d := b.Initial
	for i := 0; i < attempt && d < b.Max; i++ {
		d *= 2
	}
	if d > b.Max {
		d = b.Max
	}
	return d
}

// Policy bounds Do.
type Policy struct {
	Backoff Backoff
	// Budget is how long Do keeps trying in all: no new attempt starts once the next pause would
	// end past it.
	Budget time.Duration
	// AttemptTimeout bounds a single attempt (0 = none), so an unreachable host fails fast
	// instead of after the kernel's SYN retries.
	AttemptTimeout time.Duration
}

// Do runs op until it succeeds, fails with an error Retryable rejects, ctx ends, or the budget is
// spent. Each failed attempt is logged with err only — op must not put a DSN in its errors.
func Do[T any](ctx context.Context, p Policy, log *slog.Logger, what string, op func(context.Context) (T, error)) (T, error) {
	start := time.Now()
	for attempt := 0; ; attempt++ {
		actx, cancel := ctx, context.CancelFunc(func() {})
		if p.AttemptTimeout > 0 {
			actx, cancel = context.WithTimeout(ctx, p.AttemptTimeout)
		}
		v, err := op(actx)
		cancel()
		if err == nil {
			if attempt > 0 {
				log.Info(what+": connected after retries", "attempts", attempt+1, "after", time.Since(start).Round(time.Millisecond).String())
			}
			return v, nil
		}
		if ctx.Err() != nil {
			return v, ctx.Err()
		}
		if !Retryable(err) {
			return v, err
		}
		delay := p.Backoff.Delay(attempt)
		if time.Since(start)+delay > p.Budget {
			return v, fmt.Errorf("%s: gave up after %d attempts in %s: %w", what, attempt+1, time.Since(start).Round(time.Second), err)
		}
		log.Warn(what+" failed; retrying", "attempt", attempt+1, "in", delay.String(), "err", err)
		if !Sleep(ctx, delay) {
			return v, ctx.Err()
		}
	}
}

// Sleep waits d or until ctx ends; false means ctx ended.
func Sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
