package wa

import (
	"context"
	"encoding/json"
	"strconv"
	"time"

	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"

	"github.com/TacticalDevsBattalion/k2-system-whatsapp/internal/ids"
	"github.com/TacticalDevsBattalion/k2-system-whatsapp/internal/store"
)

// Event is the published JSON body, field for field the 1.x one.
type Event struct {
	Event        string  `json:"event"`
	ID           string  `json:"id"`
	ChatID       string  `json:"chatId"`
	From         string  `json:"from"`
	Author       string  `json:"author,omitempty"`
	Body         *string `json:"body,omitempty"`
	Type         string  `json:"type,omitempty"`
	Timestamp    int64   `json:"timestamp"`
	HasMedia     bool    `json:"hasMedia"`
	PreviousBody *string `json:"previousBody,omitempty"`
	EditedAt     int64   `json:"editedAt,omitempty"`
	DeletedAt    int64   `json:"deletedAt,omitempty"`
}

// compat writes a user JID in the 1.x dialect, preferring the phone number: a @lid sender is
// replaced by the number WhatsApp paired it with when the account knows it.
func (m *Manager) compat(ctx context.Context, j, alt types.JID) string {
	j = j.ToNonAD()
	if j.Server == types.HiddenUserServer {
		if !alt.IsEmpty() && alt.Server == types.DefaultUserServer {
			return ids.FromJID(alt.ToNonAD())
		}
		if cli := m.cli(); cli != nil {
			if pn, err := cli.Store.LIDs.GetPNForLID(ctx, j); err == nil && !pn.IsEmpty() {
				return ids.FromJID(pn.ToNonAD())
			}
		}
	}
	return ids.FromJID(j)
}

// chatOf is the 1.x chat id of a message. A one-to-one chat addressed by LID is reported under
// the counterpart's number, as 1.x did, so the allowlist and the consumers keep matching it.
func (m *Manager) chatOf(ctx context.Context, info types.MessageInfo) string {
	if info.Chat.Server == types.HiddenUserServer && !info.IsGroup {
		alt := info.SenderAlt
		if info.IsFromMe {
			alt = info.RecipientAlt
		}
		return m.compat(ctx, info.Chat, alt)
	}
	return ids.FromJID(info.Chat)
}

func (m *Manager) allowed(chat string) bool { return len(m.allow) == 0 || m.allow[chat] }

func (m *Manager) handleMessage(evt *events.Message) {
	ctx := context.Background()
	info := evt.Info
	if info.IsFromMe {
		// 1.x forwarded only what others wrote (whatsapp-web.js `message`, not `message_create`).
		return
	}
	chat := m.chatOf(ctx, info)
	author := ""
	if info.IsGroup || info.Chat.Server == types.BroadcastServer {
		author = m.compat(ctx, info.Sender, info.SenderAlt)
	}

	if pm := evt.Message.GetProtocolMessage(); pm != nil {
		switch pm.GetType() {
		case waE2E.ProtocolMessage_REVOKE:
			m.handleRevoke(ctx, chat, author, info, pm)
		case waE2E.ProtocolMessage_MESSAGE_EDIT:
			m.handleEdit(ctx, chat, author, info, pm)
		}
		return
	}

	c, ok := describe(evt.Message)
	if !ok {
		return
	}
	key := ids.MessageKey{Chat: chat, ID: info.ID, Participant: author}
	body := c.Body
	raw, _ := proto.Marshal(evt.Message)
	rec := store.Message{
		Chat: chat, ID: info.ID, Sender: firstNonEmpty(author, chat), Time: info.Timestamp,
		Type: c.Type, Body: &body, HasMedia: c.HasMedia, Raw: raw,
	}
	var out *store.OutboxEvent
	if m.allowed(chat) {
		out = outbox(chat, "created", key.String(), Event{
			Event: "created", ID: key.String(), ChatID: chat, From: chat, Author: author,
			Body: &body, Type: c.Type, Timestamp: info.Timestamp.Unix(), HasMedia: c.HasMedia,
		})
	}
	fresh, err := retryDB(func() (bool, error) { return m.st.SaveInbound(ctx, rec, out) })
	if err != nil {
		m.log.Error("Inbound message NOT recorded — it will be lost if WhatsApp does not redeliver", "chatId", chat, "id", info.ID, "err", err)
		return
	}
	m.touchChat(ctx, chat, info.Timestamp, info.PushName, info.IsGroup)
	switch {
	case !fresh:
		m.log.Debug("Inbound message already recorded (redelivery)", "chatId", chat, "id", info.ID)
	case out == nil:
		m.log.Debug("Inbound message not forwarded: chat not in allowlist", "chatId", chat)
	default:
		m.log.Info("Message received", "chatId", chat, "messageId", key.String(), "type", c.Type, "hasMedia", c.HasMedia)
		m.out.Notify()
	}
}

func (m *Manager) handleEdit(ctx context.Context, chat, author string, info types.MessageInfo, pm *waE2E.ProtocolMessage) {
	origID := pm.GetKey().GetID()
	c, ok := describe(pm.GetEditedMessage())
	if origID == "" || !ok {
		return
	}
	orig, err := m.st.FindMessage(ctx, chat, origID)
	var prev *string
	origType, origTS, hasMedia := c.Type, info.Timestamp, c.HasMedia
	if err == nil {
		prev, origType, origTS, hasMedia = orig.Body, firstNonEmpty(orig.Type, c.Type), orig.Time, orig.HasMedia
		if prev != nil && *prev == c.Body {
			return
		}
	}
	key := ids.MessageKey{Chat: chat, ID: origID, Participant: author}
	editedMs := info.Timestamp.UnixMilli()
	body := c.Body
	var out *store.OutboxEvent
	if m.allowed(chat) {
		out = outbox(chat, "edited", key.String()+":edited:"+strconv.FormatInt(editedMs, 10), Event{
			Event: "edited", ID: key.String(), ChatID: chat, From: chat, Author: author, Body: &body,
			Type: origType, Timestamp: origTS.Unix(), HasMedia: hasMedia, PreviousBody: prev, EditedAt: info.Timestamp.Unix(),
		})
	}
	fresh, err := retryDB(func() (bool, error) { return m.st.SaveEvent(ctx, chat, origID, &body, "", out) })
	if err != nil {
		m.log.Error("Edit NOT recorded", "chatId", chat, "id", origID, "err", err)
		return
	}
	if fresh && out != nil {
		m.log.Info("Message edited", "chatId", chat, "messageId", key.String())
		m.out.Notify()
	}
}

func (m *Manager) handleRevoke(ctx context.Context, chat, author string, info types.MessageInfo, pm *waE2E.ProtocolMessage) {
	origID := pm.GetKey().GetID()
	if origID == "" {
		return
	}
	ev := Event{Event: "deleted", ChatID: chat, From: chat, Author: author, Timestamp: info.Timestamp.Unix(), DeletedAt: time.Now().Unix()}
	if orig, err := m.st.FindMessage(ctx, chat, origID); err == nil {
		ev.Body, ev.Type, ev.Timestamp, ev.HasMedia = orig.Body, orig.Type, orig.Time.Unix(), orig.HasMedia
	} else {
		// 1.x skipped a deletion of a message it never saw: there is nothing for a consumer to undo.
		m.log.Debug("Deletion of a message never recorded; skipped", "chatId", chat, "id", origID)
		return
	}
	key := ids.MessageKey{Chat: chat, ID: origID, Participant: author}
	ev.ID = key.String()
	var out *store.OutboxEvent
	if m.allowed(chat) {
		out = outbox(chat, "deleted", key.String()+":deleted", ev)
	}
	fresh, err := retryDB(func() (bool, error) { return m.st.SaveEvent(ctx, chat, origID, nil, "revoked", out) })
	if err != nil {
		m.log.Error("Deletion NOT recorded", "chatId", chat, "id", origID, "err", err)
		return
	}
	if fresh && out != nil {
		m.log.Info("Message deleted", "chatId", chat, "messageId", key.String())
		m.out.Notify()
	}
}

func outbox(chat, event, amqpID string, e Event) *store.OutboxEvent {
	payload, _ := json.Marshal(e)
	return &store.OutboxEvent{AMQPMessageID: amqpID, Chat: chat, Event: event, Payload: payload}
}

// retryDB keeps trying a write for about two minutes. The handler blocks WhatsApp's event loop
// meanwhile, and the message is acked only after it returns — a short Postgres failover delays
// delivery instead of dropping it.
func retryDB(f func() (bool, error)) (bool, error) {
	var err error
	delay := 500 * time.Millisecond
	for attempt := 0; attempt < 10; attempt++ {
		var ok bool
		if ok, err = f(); err == nil {
			return ok, nil
		}
		time.Sleep(delay)
		if delay < 30*time.Second {
			delay *= 2
		}
	}
	return false, err
}

func (m *Manager) touchChat(ctx context.Context, chat string, ts time.Time, pushName string, isGroup bool) {
	c := store.Chat{ID: chat, LastTime: &ts}
	if !isGroup && pushName != "" {
		c.Name = &pushName
	}
	if err := m.st.UpsertChat(ctx, c); err != nil {
		m.log.Debug("chat upsert failed", "chatId", chat, "err", err)
	}
}

// handleHistorySync records the conversations the phone shares on link, which is what GET /chats
// lists. Messages from history are not forwarded: they were sent before this device existed.
func (m *Manager) handleHistorySync(evt *events.HistorySync) {
	ctx := context.Background()
	n := 0
	for _, conv := range evt.Data.GetConversations() {
		jid, err := types.ParseJID(conv.GetID())
		if err != nil {
			continue
		}
		chat := ids.FromJID(jid)
		if jid.Server == types.HiddenUserServer {
			chat = m.compat(ctx, jid, types.EmptyJID)
		}
		c := store.Chat{ID: chat}
		if name := firstNonEmpty(conv.GetName(), conv.GetDisplayName()); name != "" {
			c.Name = &name
		}
		if ts := conv.GetConversationTimestamp(); ts > 0 {
			t := time.Unix(int64(ts), 0)
			c.LastTime = &t
		}
		archived := conv.GetArchived()
		c.Archived = &archived
		unread := int(conv.GetUnreadCount())
		c.UnreadCount = &unread
		if err := m.st.UpsertChat(ctx, c); err == nil {
			n++
		}
	}
	if n > 0 {
		m.log.Info("History sync: chats recorded", "type", evt.Data.GetSyncType().String(), "chats", n)
	}
}

// messageForRetry answers a recipient's retry receipt with the message as it was sent, after a
// restart too (whatsmeow's own retry store covers the common case; this is the fallback).
func (m *Manager) messageForRetry(_, to types.JID, id types.MessageID) *waE2E.Message {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	rec, err := m.st.FindMessage(ctx, "", id)
	if err != nil || len(rec.Raw) == 0 {
		return nil
	}
	var msg waE2E.Message
	if err := proto.Unmarshal(rec.Raw, &msg); err != nil {
		return nil
	}
	m.log.Info("Answering a retry receipt from the store", "to", to.String(), "id", id)
	return &msg
}
