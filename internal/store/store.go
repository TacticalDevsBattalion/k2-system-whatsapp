// Package store keeps everything the gateway must not lose across restarts in Postgres:
//
//   - the WhatsApp session itself (whatsmeow's own whatsmeow_* tables, see Container);
//   - wa_messages: inbound and sent messages for a week — what a reply quotes, what an edit or a
//     deletion is reported against, and what a recipient's retry receipt is answered from;
//   - wa_outbox: every event owed to RabbitMQ, written in the same transaction that records the
//     message, before WhatsApp gets its ack — so a crash, a broker outage or a redeploy can delay
//     an event but never drop it;
//   - wa_idempotency: the callers' idempotency keys.
//
// The tables live in whatever schema the role's search_path points at (wa_reader in production).
package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"hash/fnv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib" // registers the "pgx" database/sql driver for whatsmeow
)

type Store struct {
	Pool *pgxpool.Pool
	// SQL is a database/sql handle on the same database, for whatsmeow's sqlstore.
	SQL *sql.DB
	dsn string
}

func Open(ctx context.Context, dsn string) (*Store, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("postgres pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("postgres ping: %w", err)
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		pool.Close()
		return nil, fmt.Errorf("postgres sql: %w", err)
	}
	db.SetMaxOpenConns(4)
	return &Store{Pool: pool, SQL: db, dsn: dsn}, nil
}

func (s *Store) Close() {
	_ = s.SQL.Close()
	s.Pool.Close()
}

// migrations are applied in order, each once, recorded in wa_schema_version.
var migrations = []string{
	`create table if not exists wa_messages (
		chat_id   text        not null,
		id        text        not null,
		sender    text        not null default '',
		from_me   boolean     not null default false,
		ts        timestamptz not null,
		type      text        not null default '',
		body      text,
		has_media boolean     not null default false,
		raw       bytea,
		primary key (chat_id, id)
	);
	create index if not exists wa_messages_id on wa_messages (id);
	create index if not exists wa_messages_ts on wa_messages (ts);

	create table if not exists wa_outbox (
		seq             bigserial   primary key,
		amqp_message_id text        not null unique,
		chat_id         text        not null,
		event           text        not null,
		payload         jsonb       not null,
		created_at      timestamptz not null default now(),
		published_at    timestamptz
	);
	create index if not exists wa_outbox_pending on wa_outbox (seq) where published_at is null;

	-- whatsmeow has no getChats(): the chat list is built from history sync and live traffic.
	create table if not exists wa_chats (
		chat_id      text        primary key,
		name         text,
		last_ts      timestamptz,
		archived     boolean,
		unread_count integer
	);

	create table if not exists wa_idempotency (
		key           text        primary key,
		chat_id       text,
		sent_messages integer,
		message_id    text,
		claimed_at    timestamptz not null default now(),
		completed_at  timestamptz
	);`,
}

func (s *Store) Migrate(ctx context.Context) error {
	if _, err := s.Pool.Exec(ctx, `create table if not exists wa_schema_version (version integer not null)`); err != nil {
		return fmt.Errorf("schema version table: %w", err)
	}
	var current int
	if err := s.Pool.QueryRow(ctx, `select coalesce(max(version), 0) from wa_schema_version`).Scan(&current); err != nil {
		return err
	}
	for i := current; i < len(migrations); i++ {
		err := pgx.BeginFunc(ctx, s.Pool, func(tx pgx.Tx) error {
			if _, err := tx.Exec(ctx, migrations[i]); err != nil {
				return err
			}
			_, err := tx.Exec(ctx, `insert into wa_schema_version (version) values ($1)`, i+1)
			return err
		})
		if err != nil {
			return fmt.Errorf("migration %d: %w", i+1, err)
		}
	}
	return nil
}

// OwnerLock is a session advisory lock held on a dedicated connection. While it is held this
// process is the only one allowed to connect the WhatsApp session; WhatsApp itself replaces one
// of two connections on the same device ("stream replaced"), and two would fight for ever.
type OwnerLock struct {
	conn *pgx.Conn
	// Lost is closed when the lock's connection dies; the holder must stop using the session.
	Lost chan struct{}
}

// AcquireOwnerLock blocks until the lock for clientID is free or ctx ends. onWait is called once
// if the lock is busy, so the caller can say why it is not starting.
func (s *Store) AcquireOwnerLock(ctx context.Context, clientID string, onWait func()) (*OwnerLock, error) {
	conn, err := pgx.Connect(ctx, s.dsn)
	if err != nil {
		return nil, fmt.Errorf("lock connection: %w", err)
	}
	// If this pod's node drops off the network, Postgres must notice the dead connection and free
	// the lock in seconds, not after the kernel's two-hour keepalive default.
	if _, err := conn.Exec(ctx, `set tcp_keepalives_idle = 10; set tcp_keepalives_interval = 5; set tcp_keepalives_count = 3`); err != nil {
		_ = conn.Close(context.Background())
		return nil, fmt.Errorf("lock keepalives: %w", err)
	}
	key := lockKey(clientID)
	waited := false
	for {
		var ok bool
		if err := conn.QueryRow(ctx, `select pg_try_advisory_lock($1)`, key).Scan(&ok); err != nil {
			_ = conn.Close(context.Background())
			return nil, fmt.Errorf("advisory lock: %w", err)
		}
		if ok {
			break
		}
		if !waited && onWait != nil {
			onWait()
			waited = true
		}
		select {
		case <-ctx.Done():
			_ = conn.Close(context.Background())
			return nil, ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
	l := &OwnerLock{conn: conn, Lost: make(chan struct{})}
	go l.watch()
	return l, nil
}

func (l *OwnerLock) watch() {
	defer close(l.Lost)
	for {
		time.Sleep(5 * time.Second)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := l.conn.Ping(ctx)
		cancel()
		if err != nil {
			return
		}
	}
}

func (l *OwnerLock) Release() {
	_ = l.conn.Close(context.Background())
}

func lockKey(clientID string) int64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte("k2-wa-reader/" + clientID))
	return int64(h.Sum64())
}

// ErrNotFound is returned when a looked-up row does not exist.
var ErrNotFound = errors.New("not found")
