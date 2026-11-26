package whatsmeow_service

import (
	"sync"
	"testing"
)

// A pairing nobody scanned used to restart the client after its kill: a new device, new QR codes, a new timeout,
// forever (about 1,700 loops a day for one abandoned number). After a QR timeout the client stays down until a QR is
// asked for again; every other kill (a logout, a reconnect) still restarts it.
func TestRestartAfterKill(t *testing.T) {
	t.Run("a kill that is not a QR timeout restarts the client", func(t *testing.T) {
		mycli := &MyClient{}
		if !mycli.restartAfterKill() {
			t.Fatal("expected a restart after a plain kill")
		}
	})

	t.Run("a QR timeout does not restart the client", func(t *testing.T) {
		mycli := &MyClient{}
		mycli.markQRTimedOut()
		if mycli.restartAfterKill() {
			t.Fatal("expected no restart after a QR timeout")
		}
	})

	t.Run("a new client starts without the mark", func(t *testing.T) {
		old := &MyClient{}
		old.markQRTimedOut()
		fresh := &MyClient{}
		if !fresh.restartAfterKill() {
			t.Fatal("the mark of an old client leaked into a new one")
		}
	})

	t.Run("marking and reading from many goroutines is safe", func(t *testing.T) {
		mycli := &MyClient{}
		var wg sync.WaitGroup
		for i := 0; i < 64; i++ {
			wg.Add(2)
			go func() { defer wg.Done(); mycli.markQRTimedOut() }()
			go func() { defer wg.Done(); _ = mycli.restartAfterKill() }()
		}
		wg.Wait()
		if mycli.restartAfterKill() {
			t.Fatal("expected no restart once marked")
		}
	})
}
