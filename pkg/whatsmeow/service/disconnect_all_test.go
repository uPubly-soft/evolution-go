package whatsmeow_service

import (
	"testing"

	"go.mau.fi/whatsmeow"
)

// Shutdown disconnects every number before it waits for the webhooks: WhatsApp keeps what arrives next for the
// reconnect instead of handing it to a process that is about to exit.
func TestDisconnectAllWithNoClients(t *testing.T) {
	w := &whatsmeowService{clientPointer: map[string]*whatsmeow.Client{}}
	if n := w.DisconnectAll(); n != 0 {
		t.Fatalf("expected 0 disconnected, got %d", n)
	}
}

// A slot left nil by a client that is starting or being cleaned up is skipped, not dereferenced.
func TestDisconnectAllSkipsNilClients(t *testing.T) {
	w := &whatsmeowService{clientPointer: map[string]*whatsmeow.Client{"a": nil, "b": nil}}
	if n := w.DisconnectAll(); n != 0 {
		t.Fatalf("expected 0 disconnected, got %d", n)
	}
}

func TestDisconnectAllWithNilMap(t *testing.T) {
	w := &whatsmeowService{}
	if n := w.DisconnectAll(); n != 0 {
		t.Fatalf("expected 0 disconnected, got %d", n)
	}
}
