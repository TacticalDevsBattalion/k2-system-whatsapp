// Command wa-reader is the K2 WhatsApp API Gateway, 2.x: REST for sending, RabbitMQ for inbound
// messages, one linked WhatsApp device driven through whatsmeow.
//
// Start order: the HTTP server comes up at once (health, Swagger, session endpoints answer while
// WhatsApp connects), then the process waits for the owner lock in Postgres, and only the owner
// connects WhatsApp and drains the outbox. A second replica therefore idles as a warm standby
// and takes over within seconds when the owner goes away.
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
	"sync/atomic"
	"syscall"
	"time"

	"github.com/TacticalDevsBattalion/k2-system-whatsapp/internal/api"
	"github.com/TacticalDevsBattalion/k2-system-whatsapp/internal/config"
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

	st, err := store.Open(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer st.Close()
	if err := st.Migrate(ctx); err != nil {
		return err
	}

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

	lock, err := st.AcquireOwnerLock(ctx, cfg.ClientID, func() {
		log.Info("Another replica owns the WhatsApp session; standing by", "clientId", cfg.ClientID)
	})
	if err != nil {
		if ctx.Err() != nil {
			return shutdown(srv, nil, nil, log)
		}
		return err
	}
	h.owner.Store(true)
	log.Info("Owner lock acquired", "clientId", cfg.ClientID)

	if err := mgr.Start(ctx); err != nil {
		log.Error("WhatsApp did not start; the session endpoints stay up", "err", err)
	}
	go pub.Run(ctx)
	go prune(ctx, st, cfg, log)

	select {
	case <-ctx.Done():
		log.Info("Shutting down")
	case err := <-srvErr:
		_ = shutdown(srv, mgr, lock, log)
		return err
	case <-lock.Lost:
		log.Error("Lost the owner lock (database connection gone); stopping so another replica can take over")
		_ = shutdown(srv, mgr, lock, log)
		return errors.New("owner lock lost")
	}
	return shutdown(srv, mgr, lock, log)
}

func shutdown(srv *http.Server, mgr *wa.Manager, lock *store.OwnerLock, log *slog.Logger) error {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	err := srv.Shutdown(ctx)
	if mgr != nil {
		mgr.Stop()
	}
	if lock != nil {
		lock.Release()
	}
	log.Info("Stopped")
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
