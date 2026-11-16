// Package rabbit drains the outbox into RabbitMQ.
//
// The wire format is the 1.x one: a durable headers exchange (RABBITMQ_EXCHANGE), routing key
// "", the payload as JSON, headers chatId and event, AMQP messageId <id> / <id>:edited:<ms> /
// <id>:deleted, persistent delivery. Consumers (report-parser) bind on the chatId header.
//
// Unlike 1.x, an event is deleted from the owed list only once the broker has confirmed it, and
// the list lives in Postgres — so a broker outage, a missing RABBITMQ_URL or a restart delays
// events instead of losing them (the 13–15.11.2026 gap cannot recur this way).
package rabbit

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/TacticalDevsBattalion/k2-system-whatsapp/internal/config"
	"github.com/TacticalDevsBattalion/k2-system-whatsapp/internal/store"
)

type Publisher struct {
	cfg   config.RabbitConfig
	store *store.Store
	log   *slog.Logger
	wake  chan struct{}

	mu        sync.RWMutex
	connected bool
}

func New(cfg config.RabbitConfig, st *store.Store, log *slog.Logger) *Publisher {
	return &Publisher{cfg: cfg, store: st, log: log, wake: make(chan struct{}, 1)}
}

// Enabled reports whether a broker is configured at all.
func (p *Publisher) Enabled() bool { return p.cfg.URL != "" }

// Connected reports whether a confirm channel is open right now.
func (p *Publisher) Connected() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.connected
}

// Notify wakes the pump after a new event was written to the outbox.
func (p *Publisher) Notify() {
	select {
	case p.wake <- struct{}{}:
	default:
	}
}

// Run keeps a connection open and drains the outbox until ctx ends.
func (p *Publisher) Run(ctx context.Context) {
	if !p.Enabled() {
		p.log.Warn("RABBITMQ_URL not set — events are kept in the outbox and published once a broker is configured")
		return
	}
	for ctx.Err() == nil {
		err := p.session(ctx)
		p.setConnected(false)
		if ctx.Err() != nil {
			return
		}
		p.log.Warn("RabbitMQ session ended; reconnecting", "err", err, "in", p.cfg.ReconnectDelay)
		select {
		case <-ctx.Done():
			return
		case <-time.After(p.cfg.ReconnectDelay):
		}
	}
}

func (p *Publisher) setConnected(v bool) {
	p.mu.Lock()
	p.connected = v
	p.mu.Unlock()
}

func (p *Publisher) session(ctx context.Context) error {
	conn, err := amqp.DialConfig(p.cfg.URL, amqp.Config{
		Dial:       amqp.DefaultDial(p.cfg.ConnectTimeout),
		Heartbeat:  10 * time.Second,
		Properties: amqp.Table{"connection_name": "wa-reader"},
	})
	if err != nil {
		return err
	}
	defer conn.Close()
	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()
	if err := ch.Confirm(false); err != nil {
		return err
	}
	if err := ch.ExchangeDeclare(p.cfg.Exchange, amqp.ExchangeHeaders, true, false, false, false, nil); err != nil {
		return err
	}
	closed := conn.NotifyClose(make(chan *amqp.Error, 1))
	p.setConnected(true)
	p.log.Info("Connected to RabbitMQ", "exchange", p.cfg.Exchange)

	tick := time.NewTicker(5 * time.Second)
	defer tick.Stop()
	for {
		if err := p.drain(ctx, ch); err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return nil
		case e := <-closed:
			if e == nil {
				return errors.New("connection closed")
			}
			return e
		case <-p.wake:
		case <-tick.C:
		}
	}
}

// drain publishes owed events oldest first, one at a time with a confirm each: order matters to a
// consumer reading a chat, and the volume is a few messages a minute.
func (p *Publisher) drain(ctx context.Context, ch *amqp.Channel) error {
	for {
		events, err := p.store.PendingOutbox(ctx, p.cfg.PublishBatch)
		if err != nil {
			return err
		}
		if len(events) == 0 {
			return nil
		}
		for _, e := range events {
			conf, err := ch.PublishWithDeferredConfirmWithContext(ctx, p.cfg.Exchange, "", false, false, amqp.Publishing{
				Headers:      amqp.Table{"chatId": e.Chat, "event": e.Event},
				ContentType:  "application/json",
				DeliveryMode: amqp.Persistent,
				MessageId:    e.AMQPMessageID,
				Timestamp:    time.Now(),
				Body:         e.Payload,
			})
			if err != nil {
				return err
			}
			wctx, cancel := context.WithTimeout(ctx, 30*time.Second)
			ok, err := conf.WaitContext(wctx)
			cancel()
			if err != nil {
				return err
			}
			if !ok {
				return errors.New("broker nacked publish " + e.AMQPMessageID)
			}
			if err := p.store.MarkPublished(ctx, e.Seq); err != nil {
				return err
			}
			p.log.Info("Event published", "event", e.Event, "chatId", e.Chat, "messageId", e.AMQPMessageID)
		}
		if len(events) < p.cfg.PublishBatch {
			return nil
		}
	}
}
