// Package wa owns the WhatsApp session: one whatsmeow client, linked as a companion device,
// talking WhatsApp's own protocol over a websocket — no browser.
package wa

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/skip2/go-qrcode"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waCompanionReg"
	waStore "go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/store/sqlstore"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"

	"github.com/TacticalDevsBattalion/k2-system-whatsapp/internal/config"
	"github.com/TacticalDevsBattalion/k2-system-whatsapp/internal/store"
)

// Status is the session state, with the 1.x values.
type Status string

const (
	StatusInitializing  Status = "initializing"
	StatusQR            Status = "qr"
	StatusAuthenticated Status = "authenticated"
	StatusReady         Status = "ready"
	StatusDisconnected  Status = "disconnected"
	StatusLoggedOut     Status = "logged_out"
)

// Notifier is told when an event was written to the outbox.
type Notifier interface{ Notify() }

type Manager struct {
	cfg       config.Config
	st        *store.Store
	out       Notifier
	log       *slog.Logger
	waLog     waLog.Logger
	container *sqlstore.Container
	allow     map[string]bool

	mu        sync.RWMutex
	client    *whatsmeow.Client
	status    Status
	qrCode    string
	qrPNG     []byte
	qrCancel  context.CancelFunc
	statusChg chan struct{}
	lastError string

	sendMu   sync.Mutex
	lastSend time.Time

	groups groupCache
}

func NewManager(cfg config.Config, st *store.Store, out Notifier, log *slog.Logger) *Manager {
	allow := map[string]bool{}
	for _, id := range cfg.Rabbit.ChatIDs {
		allow[id] = true
	}
	return &Manager{
		cfg: cfg, st: st, out: out, log: log, waLog: slogAdapter{log.With("module", "whatsmeow")},
		allow: allow, status: StatusInitializing, statusChg: make(chan struct{}),
	}
}

// Start opens the session store and connects. A linked device reconnects silently; without one
// the manager starts a QR login right away, as 1.x did on first run.
func (m *Manager) Start(ctx context.Context) error {
	waStore.SetOSInfo(m.cfg.DeviceName, [3]uint32{2, 0, 0})
	waStore.DeviceProps.PlatformType = waCompanionReg.DeviceProps_DESKTOP.Enum()
	m.container = sqlstore.NewWithDB(m.st.SQL, "postgres", m.waLog.Sub("store"))
	if err := m.container.Upgrade(ctx); err != nil {
		return fmt.Errorf("whatsmeow store upgrade: %w", err)
	}
	device, err := m.container.GetFirstDevice(ctx)
	if err != nil {
		return fmt.Errorf("load device: %w", err)
	}
	m.newClient(device)
	return m.connect(context.Background())
}

func (m *Manager) newClient(device *waStore.Device) {
	cli := whatsmeow.NewClient(device, m.waLog)
	// Ack an inbound message only after the handler has written it (and its event) to Postgres:
	// a crash before that makes WhatsApp redeliver instead of the message being lost.
	cli.SynchronousAck = true
	cli.EnableDecryptedEventBuffer = true
	cli.UseRetryMessageStore = true
	cli.AutomaticMessageRerequestFromPhone = true
	cli.GetMessageForRetry = m.messageForRetry
	cli.AddEventHandler(m.handleEvent)
	m.mu.Lock()
	m.client = cli
	m.mu.Unlock()
}

func (m *Manager) cli() *whatsmeow.Client {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.client
}

func (m *Manager) connect(ctx context.Context) error {
	cli := m.cli()
	if cli.Store.ID == nil {
		return m.startQR(ctx)
	}
	m.setStatus(StatusInitializing)
	if err := cli.Connect(); err != nil {
		// whatsmeow reconnects on its own only after a first successful connection: until then a
		// network outage at start would leave the gateway down for good. Keep trying.
		m.setStatus(StatusDisconnected)
		m.log.Warn("WhatsApp connect failed; retrying", "err", err)
		go m.retryConnect(ctx, cli)
	}
	return nil
}

func (m *Manager) retryConnect(ctx context.Context, cli *whatsmeow.Client) {
	delay := 2 * time.Second
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(delay):
		}
		if m.cli() != cli || cli.IsConnected() || cli.Store.ID == nil {
			return // replaced by a login/logout, or connected meanwhile
		}
		err := cli.Connect()
		if err == nil || errors.Is(err, whatsmeow.ErrAlreadyConnected) {
			return
		}
		m.log.Warn("WhatsApp connect failed; retrying", "err", err, "in", (delay * 2).String())
		if delay < time.Minute {
			delay *= 2
		}
	}
}

// startQR connects an unlinked client and keeps the current QR available until it is scanned or
// WhatsApp stops issuing codes (about two minutes), after which the session is logged out until
// the next POST /session/login.
func (m *Manager) startQR(ctx context.Context) error {
	cli := m.cli()
	qctx, cancel := context.WithCancel(context.Background())
	ch, err := cli.GetQRChannel(qctx)
	if err != nil {
		cancel()
		return fmt.Errorf("qr channel: %w", err)
	}
	m.mu.Lock()
	if m.qrCancel != nil {
		m.qrCancel()
	}
	m.qrCancel = cancel
	m.mu.Unlock()
	if err := cli.Connect(); err != nil {
		cancel()
		m.setStatus(StatusLoggedOut)
		return fmt.Errorf("connect for qr: %w", err)
	}
	go func() {
		for item := range ch {
			switch item.Event {
			case whatsmeow.QRChannelEventCode:
				png, err := qrcode.Encode(item.Code, qrcode.Medium, 512)
				if err != nil {
					m.log.Warn("QR render failed", "err", err)
				}
				m.mu.Lock()
				m.qrCode, m.qrPNG = item.Code, png
				m.mu.Unlock()
				m.setStatus(StatusQR)
				m.log.Info("Login QR ready — scan it at GET /qr (WhatsApp ▸ Linked devices)", "validFor", item.Timeout.String())
			case whatsmeow.QRChannelSuccess.Event:
				m.clearQR()
				m.setStatus(StatusAuthenticated)
				m.log.Info("QR scanned; device linked")
			case whatsmeow.QRChannelTimeout.Event:
				m.clearQR()
				cli.Disconnect()
				m.setStatus(StatusLoggedOut)
				m.log.Warn("Login QR expired without a scan; POST /session/login to get a new one")
			default:
				m.clearQR()
				if item.Error != nil {
					m.setError(item.Error.Error())
				}
				m.log.Warn("Login did not complete", "event", item.Event, "err", item.Error)
				if cli.Store.ID == nil {
					cli.Disconnect()
					m.setStatus(StatusLoggedOut)
				}
			}
		}
	}()
	return nil
}

func (m *Manager) clearQR() {
	m.mu.Lock()
	m.qrCode, m.qrPNG = "", nil
	m.mu.Unlock()
}

func (m *Manager) setError(s string) {
	m.mu.Lock()
	m.lastError = s
	m.mu.Unlock()
}

func (m *Manager) setStatus(s Status) {
	m.mu.Lock()
	if m.status != s {
		m.status = s
		close(m.statusChg)
		m.statusChg = make(chan struct{})
	}
	m.mu.Unlock()
}

// Status is the current session state.
func (m *Manager) Status() Status {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.status
}

// QR returns the pending login QR as a PNG, or nil.
func (m *Manager) QR() []byte {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.qrPNG
}

// Ready reports whether a send can go out now.
func (m *Manager) Ready() bool {
	cli := m.cli()
	return m.Status() == StatusReady && cli != nil && cli.IsLoggedIn()
}

// Linked reports whether a device is linked (the session survives a restart).
func (m *Manager) Linked() bool {
	cli := m.cli()
	return cli != nil && cli.Store.ID != nil
}

// waitFor blocks until the status is one of want, or the timeout passes.
func (m *Manager) waitFor(ctx context.Context, timeout time.Duration, want ...Status) Status {
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for {
		m.mu.RLock()
		s, chg := m.status, m.statusChg
		m.mu.RUnlock()
		for _, w := range want {
			if s == w {
				return s
			}
		}
		select {
		case <-chg:
		case <-deadline.C:
			return s
		case <-ctx.Done():
			return s
		}
	}
}

// Login starts a session if none is running and waits until it shows a QR or is ready.
func (m *Manager) Login(ctx context.Context) (Status, error) {
	switch m.Status() {
	case StatusReady, StatusAuthenticated, StatusQR:
		return m.Status(), nil
	}
	cli := m.cli()
	if cli.Store.ID != nil {
		if !cli.IsConnected() {
			if err := cli.Connect(); err != nil {
				return m.Status(), err
			}
		}
		return m.waitFor(ctx, m.cfg.LoginWait, StatusReady), nil
	}
	if cli.IsConnected() {
		cli.Disconnect()
	}
	if err := m.startQR(ctx); err != nil {
		return m.Status(), err
	}
	return m.waitFor(ctx, m.cfg.LoginWait, StatusQR, StatusReady, StatusAuthenticated), nil
}

// Logout unlinks the device from the account and drops the session. It stays logged out until
// the next Login. unlinked is false when WhatsApp could not be told (the local session is
// dropped anyway, and the phone may still list the device until it is removed there).
func (m *Manager) Logout(ctx context.Context) (unlinked bool, err error) {
	cli := m.cli()
	m.mu.Lock()
	if m.qrCancel != nil {
		m.qrCancel()
		m.qrCancel = nil
	}
	m.mu.Unlock()
	m.clearQR()
	if cli.Store.ID != nil {
		if err := cli.Logout(ctx); err == nil {
			unlinked = true
		} else {
			m.log.Warn("WhatsApp did not confirm the unlink; dropping the local session anyway", "err", err)
			cli.Disconnect()
			if derr := cli.Store.Delete(ctx); derr != nil {
				m.log.Warn("Local session could not be deleted", "err", derr)
			}
		}
	} else {
		cli.Disconnect()
	}
	m.newClient(m.container.NewDevice())
	m.setStatus(StatusLoggedOut)
	return unlinked, nil
}

// PairCode links the device by an 8-character code typed on the phone instead of a QR scan
// (WhatsApp ▸ Linked devices ▸ Link with phone number). A QR login must be pending.
func (m *Manager) PairCode(ctx context.Context, phone string) (string, error) {
	if m.Status() != StatusQR {
		if _, err := m.Login(ctx); err != nil {
			return "", err
		}
	}
	if m.Status() != StatusQR {
		return "", ErrNoPendingLogin
	}
	return m.cli().PairPhone(ctx, phone, true, whatsmeow.PairClientChrome, "Chrome (Linux)")
}

// ErrNoPendingLogin means a pairing was asked for while no login is waiting.
var ErrNoPendingLogin = errors.New("no login pending")

// Stop disconnects cleanly. The session stays linked.
func (m *Manager) Stop() {
	if cli := m.cli(); cli != nil {
		cli.Disconnect()
	}
}

func (m *Manager) handleEvent(evt any) {
	switch e := evt.(type) {
	case *events.Connected:
		m.setStatus(StatusReady)
		m.setError("")
		if id := m.cli().Store.ID; id != nil {
			m.log.Info("WhatsApp client is ready", "account", id.User)
		}
	case *events.PairSuccess:
		m.log.Info("Device paired", "account", e.ID.User, "platform", e.Platform, "business", e.BusinessName)
	case *events.Disconnected:
		if m.Status() == StatusReady {
			m.setStatus(StatusDisconnected)
		}
		m.log.Warn("WhatsApp disconnected; reconnecting")
	case *events.KeepAliveTimeout:
		m.log.Warn("WhatsApp keepalive timed out", "errors", e.ErrorCount)
	case *events.KeepAliveRestored:
		m.log.Info("WhatsApp keepalive restored")
	case *events.LoggedOut:
		m.log.Error("WhatsApp logged this device out; POST /session/login and scan a new QR", "onConnect", e.OnConnect, "reason", e.Reason.String())
		m.setError("logged out: " + e.Reason.String())
		go func() {
			m.newClient(m.container.NewDevice())
			m.setStatus(StatusLoggedOut)
		}()
	case *events.StreamReplaced:
		m.log.Error("Another connection took over this WhatsApp device (stream replaced); not reconnecting")
		m.setError("stream replaced")
		m.setStatus(StatusDisconnected)
	case *events.TemporaryBan:
		m.log.Error("WhatsApp temporarily banned this account", "code", e.Code.String(), "expire", e.Expire.String())
		m.setError("temporary ban: " + e.String())
		m.setStatus(StatusDisconnected)
	case *events.ConnectFailure:
		m.log.Error("WhatsApp refused the connection", "reason", e.Reason.String(), "message", e.Message)
		m.setError("connect failure: " + e.Reason.String())
	case *events.ClientOutdated:
		m.log.Error("WhatsApp says this client version is outdated — upgrade go.mau.fi/whatsmeow")
		m.setError("client outdated")
	case *events.OfflineSyncCompleted:
		m.log.Info("Offline messages delivered", "count", e.Count)
	case *events.Message:
		m.handleMessage(e)
	case *events.HistorySync:
		m.handleHistorySync(e)
	case *events.UndecryptableMessage:
		m.log.Warn("Message could not be decrypted; WhatsApp will retry", "chatId", e.Info.Chat.String(), "id", e.Info.ID, "unavailable", e.IsUnavailable)
	}
}

// LastError is the last session-level failure, for /health.
func (m *Manager) LastError() string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.lastError
}

// slogAdapter lets whatsmeow log through slog.
type slogAdapter struct{ l *slog.Logger }

func (a slogAdapter) Warnf(msg string, args ...any)  { a.l.Warn(fmt.Sprintf(msg, args...)) }
func (a slogAdapter) Errorf(msg string, args ...any) { a.l.Error(fmt.Sprintf(msg, args...)) }
func (a slogAdapter) Infof(msg string, args ...any)  { a.l.Info(fmt.Sprintf(msg, args...)) }
func (a slogAdapter) Debugf(msg string, args ...any) { a.l.Debug(fmt.Sprintf(msg, args...)) }
func (a slogAdapter) Sub(module string) waLog.Logger {
	return slogAdapter{a.l.With("module", "whatsmeow/"+module)}
}
