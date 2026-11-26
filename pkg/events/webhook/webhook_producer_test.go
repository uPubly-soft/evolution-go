package webhook_producer

import (
	"net/http"
	"net/http/httptest"
	"os"
	"sync/atomic"
	"testing"
	"time"

	config "github.com/evolution-foundation/evolution-go/pkg/config"
	logger_wrapper "github.com/evolution-foundation/evolution-go/pkg/logger"
)

var testLogDir string

func newProducer(url string) *webhookProducer {
	if testLogDir == "" {
		dir, err := os.MkdirTemp("", "webhook-producer-test")
		if err != nil {
			panic(err)
		}
		testLogDir = dir
	}
	return NewWebhookProducer(url, logger_wrapper.NewLoggerManager(&config.Config{LogDirectory: testLogDir})).(*webhookProducer)
}

// On shutdown the process waits for the webhooks already on their way: a message WhatsApp got its ack for exists
// only in that request.
func TestDrainWaitsForAnInFlightWebhook(t *testing.T) {
	release := make(chan struct{})
	var got atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-release
		got.Add(1)
	}))
	defer srv.Close()

	p := newProducer("")
	if err := p.Produce("inst.message", []byte(`{}`), srv.URL, "inst"); err != nil {
		t.Fatal(err)
	}
	time.AfterFunc(200*time.Millisecond, func() { close(release) })
	if !p.Drain(5 * time.Second) {
		t.Fatal("expected the drain to finish once the webhook was delivered")
	}
	if got.Load() != 1 {
		t.Fatalf("expected 1 delivery, got %d", got.Load())
	}
}

// A retry waiting its 30s goes at once when the drain starts, so a receiver that is back gets it before the exit.
func TestDrainCutsTheRetryWait(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			w.WriteHeader(http.StatusBadGateway)
		}
	}))
	defer srv.Close()

	p := newProducer("")
	_ = p.Produce("inst.message", []byte(`{}`), srv.URL, "inst")
	time.Sleep(200 * time.Millisecond) // the first attempt failed and the retry is waiting
	start := time.Now()
	if !p.Drain(5 * time.Second) {
		t.Fatal("expected the retry to be delivered during the drain")
	}
	if time.Since(start) > 3*time.Second {
		t.Fatal("the drain waited for the full retry interval")
	}
	if calls.Load() != 2 {
		t.Fatalf("expected 2 attempts, got %d", calls.Load())
	}
}

// A receiver that never answers cannot hold the shutdown past the timeout.
func TestDrainGivesUpAtTheTimeout(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	p := newProducer("")
	_ = p.Produce("inst.message", []byte(`{}`), srv.URL, "inst")
	start := time.Now()
	if p.Drain(500 * time.Millisecond) {
		t.Fatal("expected the drain to report deliveries still pending")
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("the drain took %v, past its timeout", d)
	}
}

func TestDrainWithNothingInFlightReturnsAtOnce(t *testing.T) {
	p := newProducer("")
	start := time.Now()
	if !p.Drain(time.Second) {
		t.Fatal("nothing in flight should drain")
	}
	if time.Since(start) > 100*time.Millisecond {
		t.Fatal("an empty drain must not wait")
	}
}

// Both the global and the instance URL count: the drain waits for each of them.
func TestDrainCountsBothUrls(t *testing.T) {
	var got atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(100 * time.Millisecond)
		got.Add(1)
	}))
	defer srv.Close()

	p := newProducer(srv.URL)
	_ = p.Produce("inst.message", []byte(`{}`), srv.URL+"/instance", "inst")
	if !p.Drain(5 * time.Second) {
		t.Fatal("expected both deliveries to drain")
	}
	if got.Load() != 2 {
		t.Fatalf("expected 2 deliveries, got %d", got.Load())
	}
}
