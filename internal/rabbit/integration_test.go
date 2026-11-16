//go:build integration

// Integration test against a real Postgres and RabbitMQ:
//
//	WA_TEST_DATABASE_URL=postgres://wa_reader:…@localhost:5432/whatsapp \
//	WA_TEST_RABBITMQ_URL=amqp://guest:guest@localhost:5672/ go test -tags integration ./internal/rabbit/
package rabbit

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/TacticalDevsBattalion/k2-system-whatsapp/internal/config"
	"github.com/TacticalDevsBattalion/k2-system-whatsapp/internal/store"
)

func env(t *testing.T, name string) string {
	v := os.Getenv(name)
	if v == "" {
		t.Skip(name + " not set")
	}
	return v
}

func TestStoreAndPublisher(t *testing.T) {
	dsn, amqpURL := env(t, "WA_TEST_DATABASE_URL"), env(t, "WA_TEST_RABBITMQ_URL")
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	st, err := store.Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	for i := 0; i < 2; i++ { // migrations are re-runnable
		if err := st.Migrate(ctx); err != nil {
			t.Fatalf("migrate #%d: %v", i, err)
		}
	}
	for _, q := range []string{"delete from wa_outbox", "delete from wa_messages", "delete from wa_idempotency"} {
		if _, err := st.Pool.Exec(ctx, q); err != nil {
			t.Fatal(err)
		}
	}

	// ── owner lock is exclusive ──
	lock, err := st.AcquireOwnerLock(ctx, "it", nil)
	if err != nil {
		t.Fatal(err)
	}
	waitCtx, waitCancel := context.WithTimeout(ctx, 3*time.Second)
	waited := false
	if _, err := st.AcquireOwnerLock(waitCtx, "it", func() { waited = true }); !errors.Is(err, context.DeadlineExceeded) || !waited {
		t.Fatalf("second owner got the lock: err=%v waited=%v", err, waited)
	}
	waitCancel()
	lock.Release()
	lock2, err := st.AcquireOwnerLock(ctx, "it", nil)
	if err != nil {
		t.Fatalf("lock not free after release: %v", err)
	}
	lock2.Release()

	// ── inbound message + event, redelivery is a no-op ──
	chat := "120363000000000000@g.us"
	body := "звіт"
	msg := store.Message{Chat: chat, ID: "3EB0TEST", Sender: "380500000000@c.us", Time: time.Unix(1763300000, 0), Type: "chat", Body: &body}
	payload, _ := json.Marshal(map[string]any{"event": "created", "id": "false_" + chat + "_3EB0TEST_380500000000@c.us", "body": body})
	ev := &store.OutboxEvent{AMQPMessageID: "false_" + chat + "_3EB0TEST_380500000000@c.us", Chat: chat, Event: "created", Payload: payload}
	if fresh, err := st.SaveInbound(ctx, msg, ev); err != nil || !fresh {
		t.Fatalf("first save: %v %v", fresh, err)
	}
	if fresh, err := st.SaveInbound(ctx, msg, ev); err != nil || fresh {
		t.Fatalf("redelivery not deduplicated: %v %v", fresh, err)
	}
	if n, _, _ := st.OutboxBacklog(ctx); n != 1 {
		t.Fatalf("backlog = %d, want 1", n)
	}
	if got, err := st.FindMessage(ctx, "", "3EB0TEST"); err != nil || got.Chat != chat || *got.Body != body {
		t.Fatalf("FindMessage: %+v %v", got, err)
	}

	// ── a consumer bound on the chatId header, as report-parser is ──
	conn, err := amqp.Dial(amqpURL)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ch, _ := conn.Channel()
	exchange := "it.whatsapp.messages"
	_ = ch.ExchangeDeclare(exchange, amqp.ExchangeHeaders, true, false, false, false, nil)
	q, _ := ch.QueueDeclare("", false, true, true, false, nil)
	_ = ch.QueueBind(q.Name, "", exchange, false, amqp.Table{"x-match": "all", "chatId": chat})
	deliveries, _ := ch.Consume(q.Name, "", true, true, false, false, nil)

	pub := New(config.RabbitConfig{URL: amqpURL, Exchange: exchange, ReconnectDelay: time.Second, ConnectTimeout: 5 * time.Second, PublishBatch: 10},
		st, slog.New(slog.NewTextHandler(io.Discard, nil)))
	runCtx, stopPub := context.WithCancel(ctx)
	go pub.Run(runCtx)
	defer stopPub()

	select {
	case d := <-deliveries:
		if d.MessageId != ev.AMQPMessageID || d.Headers["event"] != "created" || d.Headers["chatId"] != chat || d.DeliveryMode != amqp.Persistent {
			t.Fatalf("delivery: id=%q headers=%v mode=%d", d.MessageId, d.Headers, d.DeliveryMode)
		}
		var got map[string]any
		if err := json.Unmarshal(d.Body, &got); err != nil || got["body"] != body {
			t.Fatalf("payload %s: %v", d.Body, err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("event never reached the consumer")
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		if n, _, _ := st.OutboxBacklog(ctx); n == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("event not marked published")
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !pub.Connected() {
		t.Fatal("publisher reports disconnected")
	}

	// ── idempotency keys ──
	if prev, err := st.ClaimKey(ctx, "k1", time.Minute); err != nil || prev != nil {
		t.Fatalf("first claim: %v %v", prev, err)
	}
	if _, err := st.ClaimKey(ctx, "k1", time.Minute); !errors.Is(err, store.ErrInFlight) {
		t.Fatalf("in-flight claim: %v", err)
	}
	if err := st.CompleteKey(ctx, "k1", store.CompletedSend{Chat: chat, SentMessages: 2, MessageID: "true_x_y"}); err != nil {
		t.Fatal(err)
	}
	if prev, err := st.ClaimKey(ctx, "k1", time.Minute); err != nil || prev == nil || prev.SentMessages != 2 || prev.MessageID != "true_x_y" {
		t.Fatalf("replay: %+v %v", prev, err)
	}
	if _, err := st.ClaimKey(ctx, "k2", time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := st.ReleaseKey(ctx, "k2"); err != nil {
		t.Fatal(err)
	}
	if prev, err := st.ClaimKey(ctx, "k2", time.Minute); err != nil || prev != nil {
		t.Fatalf("released key not claimable: %v %v", prev, err)
	}
	// a claim whose process died is taken over after the in-flight timeout
	if _, err := st.ClaimKey(ctx, "k2", 0); err != nil {
		t.Fatalf("stale claim not taken over: %v", err)
	}
}
