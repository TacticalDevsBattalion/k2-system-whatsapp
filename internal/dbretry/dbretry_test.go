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
	"os"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func pgErr(code string) error {
	return fmt.Errorf("failed to connect to `user=wa_reader database=whatsapp`: %w", &pgconn.PgError{Severity: "FATAL", Code: code})
}

func TestRetryable(t *testing.T) {
	dial := &net.OpError{Op: "dial", Net: "tcp", Err: os.NewSyscallError("connect", syscall.ECONNREFUSED)}
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"connection refused", fmt.Errorf("postgres ping: %w", dial), true},
		{"connection reset", fmt.Errorf("read: %w", syscall.ECONNRESET), true},
		{"unexpected EOF", fmt.Errorf("receive message: %w", io.ErrUnexpectedEOF), true},
		{"dns", &net.DNSError{Err: "no such host", Name: "pg-whatsapp-rw.whatsapp", IsNotFound: true}, true},
		{"attempt timeout", fmt.Errorf("dial: %w", context.DeadlineExceeded), true},
		{"cancelled", fmt.Errorf("dial: %w", context.Canceled), false},
		{"08006 connection failure", pgErr("08006"), true},
		{"08001 cannot establish", pgErr("08001"), true},
		{"57P01 admin shutdown", pgErr("57P01"), true},
		{"57P02 crash shutdown", pgErr("57P02"), true},
		{"57P03 cannot connect now", pgErr("57P03"), true},
		{"28P01 bad password", pgErr("28P01"), false},
		{"28000 no pg_hba entry", pgErr("28000"), false},
		{"3D000 no database", pgErr("3D000"), false},
		{"42P01 undefined table", pgErr("42P01"), false},
		{"unknown CA", fmt.Errorf("tls: %w", x509.UnknownAuthorityError{}), false},
		{"hostname mismatch", fmt.Errorf("tls: %w", x509.HostnameError{Host: "x"}), false},
		{"verification", &tls.CertificateVerificationError{Err: x509.CertificateInvalidError{Reason: x509.Expired}}, false},
		{"tls fatal wins over a joined dial error", errors.Join(dial, x509.UnknownAuthorityError{}), false},
		{"unknown error", errors.New("something else"), false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := Retryable(c.err); got != c.want {
				t.Fatalf("Retryable(%v) = %v, want %v", c.err, got, c.want)
			}
		})
	}
}

// A real connect to a closed port, the way pgx wraps it.
func TestRetryableRealConnectRefused(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Skip(err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = pgx.Connect(ctx, "postgres://u:secret@"+addr+"/db?sslmode=disable")
	if err == nil {
		t.Fatal("connected to a closed port")
	}
	if !Retryable(err) {
		t.Fatalf("connection refused not retryable: %v", err)
	}
	if strings.Contains(err.Error(), "secret") {
		t.Fatalf("password in error text: %v", err)
	}
}

func TestBackoff(t *testing.T) {
	b := Backoff{Initial: time.Second, Max: 15 * time.Second}
	want := []time.Duration{1, 2, 4, 8, 15, 15, 15}
	var sum time.Duration
	for i, w := range want {
		if got := b.Delay(i); got != w*time.Second {
			t.Fatalf("Delay(%d) = %s, want %s", i, got, w*time.Second)
		}
		sum += b.Delay(i)
	}
	if sum != 60*time.Second { // the start-up schedule: about a minute in all
		t.Fatalf("sum = %s", sum)
	}
	if got := b.Delay(1000); got != 15*time.Second {
		t.Fatalf("Delay(1000) = %s", got)
	}
}

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

func TestDoRetriesUntilSuccess(t *testing.T) {
	calls := 0
	v, err := Do(context.Background(), Policy{Backoff: Backoff{Initial: time.Millisecond, Max: 4 * time.Millisecond}, Budget: time.Second},
		quiet, "test", func(context.Context) (int, error) {
			calls++
			if calls < 4 {
				return 0, pgErr("57P03")
			}
			return 42, nil
		})
	if err != nil || v != 42 || calls != 4 {
		t.Fatalf("v=%d err=%v calls=%d", v, err, calls)
	}
}

func TestDoStopsOnFatal(t *testing.T) {
	calls := 0
	_, err := Do(context.Background(), Policy{Backoff: Backoff{Initial: time.Millisecond, Max: time.Millisecond}, Budget: time.Second},
		quiet, "test", func(context.Context) (int, error) {
			calls++
			return 0, pgErr("28P01")
		})
	var pe *pgconn.PgError
	if calls != 1 || !errors.As(err, &pe) || pe.Code != "28P01" {
		t.Fatalf("calls=%d err=%v", calls, err)
	}
}

func TestDoGivesUpAfterBudget(t *testing.T) {
	calls := 0
	start := time.Now()
	_, err := Do(context.Background(), Policy{Backoff: Backoff{Initial: 5 * time.Millisecond, Max: 10 * time.Millisecond}, Budget: 50 * time.Millisecond},
		quiet, "test", func(context.Context) (int, error) {
			calls++
			return 0, syscall.ECONNREFUSED
		})
	if !errors.Is(err, syscall.ECONNREFUSED) || calls < 3 {
		t.Fatalf("calls=%d err=%v", calls, err)
	}
	if el := time.Since(start); el > 500*time.Millisecond {
		t.Fatalf("took %s", el)
	}
}

func TestDoAttemptTimeoutAndCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	calls := 0
	_, err := Do(ctx, Policy{Backoff: Backoff{Initial: time.Millisecond, Max: time.Millisecond}, Budget: time.Minute, AttemptTimeout: 5 * time.Millisecond},
		quiet, "test", func(actx context.Context) (int, error) {
			calls++
			if calls == 3 {
				cancel()
			}
			<-actx.Done() // a hanging connect, cut by the attempt timeout
			return 0, actx.Err()
		})
	if !errors.Is(err, context.Canceled) || calls != 3 {
		t.Fatalf("calls=%d err=%v", calls, err)
	}
}
