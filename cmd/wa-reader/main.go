// Command wa-reader is the K2 WhatsApp API Gateway, 2.x: REST for sending, RabbitMQ for inbound
// messages, one linked WhatsApp device driven through whatsmeow.
//
// Start order: Postgres is connected (retried for DATABASE_CONNECT_RETRY_MS while it is briefly
// away), the HTTP server comes up (health, Swagger, session endpoints answer while WhatsApp
// connects), then the process waits for the owner lock in Postgres, and only the owner connects
// WhatsApp and drains the outbox. A second replica therefore idles as a warm standby and takes
// over within seconds when the owner goes away. When the lock is lost (its connection died) the
// session pauses and the lock is taken again; see package owner.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/TacticalDevsBattalion/k2-system-whatsapp/internal/api"
	"github.com/TacticalDevsBattalion/k2-system-whatsapp/internal/config"
	"github.com/TacticalDevsBattalion/k2-system-whatsapp/internal/dbretry"
	"github.com/TacticalDevsBattalion/k2-system-whatsapp/internal/owner"
	"github.com/TacticalDevsBattalion/k2-system-whatsapp/internal/rabbit"
	"github.com/TacticalDevsBattalion/k2-system-whatsapp/internal/store"
	"github.com/TacticalDevsBattalion/k2-system-whatsapp/internal/wa"
)

// Version is set at build time (-ldflags "-X main.Version=…"); VERSION in the repo is the source.
var Version = "dev"

type health struct {
	owner atomic.Bool
	pub   *rabbit.Publisher
}

func (h *health) Owner() bool           { return h.owner.Load() }
func (h *health) RabbitEnabled() bool   { return h.pub.Enabled() }
func (h *health) RabbitConnected() bool { return h.pub.Connected() }

func main() {
	if err := run(); err != nil {
		slog.Error("wa-reader stopped", "err", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level(cfg.LogLevel)})).
		With("service", "wa-reader", "version", Version)
	slog.SetDefault(log)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	// A Postgres that is briefly away at start (a CNPG primary restart) is waited for; a wrong
	// password, a missing database or a certificate that does not verify fail at once.
	st, err := dbretry.Do(ctx, dbretry.Policy{
		Backoff:        dbretry.Backoff{Initial: time.Second, Max: 15 * time.Second},
		Budget:         cfg.DatabaseConnectRetry,
		AttemptTimeout: 15 * time.Second,
	}, log, "Postgres connect", func(ctx context.Context) (*store.Store, error) {
		st, err := store.Open(ctx, cfg.DatabaseURL)
		if err != nil {
			return nil, err
		}
		if err := st.Migrate(ctx); err != nil {
			st.Close()
			return nil, fmt.Errorf("migrate: %w", err)
		}
		return st, nil
	})
	if err != nil {
		if ctx.Err() != nil {
			return nil // stopped while waiting for Postgres
		}
		return err
	}
	defer st.Close()

	pub := rabbit.New(cfg.Rabbit, st, log.With("module", "rabbitmq"))
	mgr := wa.NewManager(cfg, st, pub, log.With("module", "whatsapp"))
	h := &health{pub: pub}

	srv := &http.Server{
		Addr:              fmt.Sprintf(":%d", cfg.Port),
		Handler:           api.New(cfg, mgr, st, h, log.With("module", "http"), Version).Handler(),
		ReadHeaderTimeout: 10 * time.Second,
		// Sends wait for the throttle and for WhatsApp's ack; uploads can be 25 MB.
		WriteTimeout: 5 * time.Minute,
	}
	srvErr := make(chan error, 1)
	go func() {
		log.Info("HTTP listening", "port", cfg.Port, "pathBase", cfg.PathBase)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			srvErr <- err
		}
	}()

	// Ownership outlives SIGTERM until HTTP is drained: in-flight sends need the session.
	ownCtx, endOwning := context.WithCancel(context.Background())
	defer endOwning()
	ownErr := make(chan error, 1)
	go func() {
		ownErr <- owner.Run(ownCtx, owner.Config{
			OutageMax: cfg.DatabaseOutageMax,
			// A short cap: once Postgres is back the session resumes within seconds.
			Backoff: dbretry.Backoff{Initial: time.Second, Max: 5 * time.Second},
		}, func(ctx context.Context, onWait func()) (owner.Lock, error) {
			l, err := st.AcquireOwnerLock(ctx, cfg.ClientID, onWait)
			if err != nil {
				return nil, err // not a typed nil in the interface
			}
			return l, nil
		}, &session{mgr: mgr, pub: pub, st: st, cfg: cfg, log: log}, h.owner.Store, log.With("clientId", cfg.ClientID))
	}()

	select {
	case <-ctx.Done():
		log.Info("Shutting down")
		// In-flight sends finish first (they need the session), then the session stops.
		err := shutdown(srv, log)
		endOwning()
		if oerr := <-ownErr; oerr != nil && err == nil {
			err = oerr
		}
		return err
	case err := <-srvErr:
		endOwning()
		<-ownErr
		return err
	case err := <-ownErr:
		// Ownership could not be regained (Postgres away too long, or a fault retrying cannot
		// fix); the session is already stopped. Exit and let Kubernetes restart us.
		_ = shutdown(srv, log)
		if err == nil {
			err = errors.New("owner loop ended")
		}
		return err
	}
}

// session is the work only the owner of the WhatsApp session does: the WhatsApp connection, the
// outbox pump and pruning. It is started on every ownership term and stopped when one ends.
type session struct {
	mgr *wa.Manager
	pub *rabbit.Publisher
	st  *store.Store
	cfg config.Config
	log *slog.Logger
	wg  sync.WaitGroup
}

func (s *session) Start(ctx context.Context) error {
	if err := s.mgr.Start(ctx); err != nil {
		return fmt.Errorf("whatsapp start: %w", err)
	}
	s.wg.Add(2)
	go func() { defer s.wg.Done(); s.pub.Run(ctx) }()
	go func() { defer s.wg.Done(); prune(ctx, s.st, s.cfg, s.log) }()
	return nil
}

// Stop returns once WhatsApp is disconnected and the pump and pruning have returned (their
// context is already cancelled by then).
func (s *session) Stop() {
	s.mgr.Stop()
	s.wg.Wait()
}

func shutdown(srv *http.Server, log *slog.Logger) error {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	err := srv.Shutdown(ctx)
	log.Info("HTTP stopped")
	return err
}

func prune(ctx context.Context, st *store.Store, cfg config.Config, log *slog.Logger) {
	t := time.NewTicker(time.Hour)
	defer t.Stop()
	for {
		if err := st.Prune(ctx, cfg.MessageTTL, cfg.IdempotencyTTL); err != nil && ctx.Err() == nil {
			log.Warn("Prune failed", "err", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

func level(s string) slog.Level {
	switch strings.ToLower(s) {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	}
	return slog.LevelInfo
}
