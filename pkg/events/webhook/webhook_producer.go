package webhook_producer

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	producer_interfaces "github.com/evolution-foundation/evolution-go/pkg/events/interfaces"
	logger_wrapper "github.com/evolution-foundation/evolution-go/pkg/logger"
)

type webhookProducer struct {
	url           string
	loggerWrapper *logger_wrapper.LoggerManager

	mu       sync.Mutex
	inFlight int
	// Closed by Drain: retries stop waiting their interval and go at once.
	draining     chan struct{}
	drainingOnce sync.Once
}

// Drainer is a producer that can wait for the deliveries it already started. Shutdown uses it after the WhatsApp
// clients are disconnected: a message WhatsApp already got its ack for exists only in that delivery.
type Drainer interface {
	Drain(timeout time.Duration) bool
}

func NewWebhookProducer(
	url string,
	loggerWrapper *logger_wrapper.LoggerManager,
) producer_interfaces.Producer {
	return &webhookProducer{
		url:           url,
		loggerWrapper: loggerWrapper,
		draining:      make(chan struct{}),
	}
}

func (p *webhookProducer) track(delta int) {
	p.mu.Lock()
	p.inFlight += delta
	p.mu.Unlock()
}

func (p *webhookProducer) pending() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.inFlight
}

// Drain makes the waiting retries go now and waits until every started delivery ends (delivered or out of attempts),
// or the timeout passes. It reports whether nothing is left in flight.
func (p *webhookProducer) Drain(timeout time.Duration) bool {
	p.drainingOnce.Do(func() { close(p.draining) })
	deadline := time.Now().Add(timeout)
	for {
		n := p.pending()
		if n == 0 {
			return true
		}
		if time.Now().After(deadline) {
			p.loggerWrapper.GetLogger("").LogWarn("[SHUTDOWN] %d webhook deliveries still pending at the drain timeout", n)
			return false
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (p *webhookProducer) Produce(
	queueName string,
	payload []byte,
	webhookUrl string,
	userID string,
) error {
	splitQueue := strings.Split(queueName, ".")

	if len(splitQueue) < 2 {
		return nil
	}

	// Counted before the goroutine starts, so a drain that begins right after Produce still waits for it.
	if p.url != "" {
		p.track(1)
		go p.sendWebhookWithRetry(p.url, payload, 5, 30*time.Second, userID)
	}
	if webhookUrl != "" {
		p.track(1)
		go p.sendWebhookWithRetry(webhookUrl, payload, 5, 30*time.Second, userID)
	}

	return nil
}

func (p *webhookProducer) sendWebhookWithRetry(url string, body []byte, maxRetries int, retryInterval time.Duration, userID string) {
	defer p.track(-1)
	for i := 0; i < maxRetries; i++ {
		err, responseBody, statusCode := p.sendWebhook(url, body, userID)
		if err == nil {
			p.loggerWrapper.GetLogger(userID).LogInfo("[%s] webhook sent successfully - url: %s, status: %d, response: %s", userID, url, statusCode, string(responseBody))
			return
		}
		p.loggerWrapper.GetLogger(userID).LogWarn("[%s] webhook failed - url: %s, attempt: %d, error: %v", userID, url, i+1, err)
		if i == maxRetries-1 {
			break
		}

		select {
		case <-time.After(retryInterval):
		case <-p.draining:
			// Shutting down: try again now rather than after the process is gone.
			time.Sleep(200 * time.Millisecond)
		}
	}
	p.loggerWrapper.GetLogger(userID).LogError("[%s] webhook failed after maximum retries - url: %s", userID, url)
}

func (p *webhookProducer) sendWebhook(url string, body []byte, userID string) (error, []byte, int) {
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(body))
	if err != nil {
		return err, nil, 0
	}

	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return err, nil, 0
	}
	defer resp.Body.Close()

	responseBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("erro ao ler resposta: %v", err), nil, 0
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return errors.New("received non-2xx response: " + resp.Status), responseBody, resp.StatusCode
	}

	return nil, responseBody, resp.StatusCode
}

// CreateGlobalQueues não faz nada para webhook producer
func (p *webhookProducer) CreateGlobalQueues() error {
	return nil
}
