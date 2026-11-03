package whatsmeow_service

import (
	"testing"
	"time"
)

func TestSendKillNilChannelReturnsAtOnce(t *testing.T) {
	start := time.Now()
	if SendKill(nil, time.Second) {
		t.Fatal("a nil channel cannot take the signal")
	}
	if time.Since(start) > 100*time.Millisecond {
		t.Fatal("a nil channel must not wait for the timeout")
	}
}

func TestSendKillClosedChannelDoesNotPanic(t *testing.T) {
	ch := make(chan bool)
	close(ch)
	if SendKill(ch, time.Second) {
		t.Fatal("a closed channel cannot take the signal")
	}
	if SendKill(ch, 0) {
		t.Fatal("a closed channel cannot take the signal")
	}
}

func TestSendKillWithoutListener(t *testing.T) {
	ch := make(chan bool)
	if SendKill(ch, 0) {
		t.Fatal("no listener: the non-blocking send must give up")
	}
	start := time.Now()
	if SendKill(ch, 50*time.Millisecond) {
		t.Fatal("no listener: the send must time out")
	}
	if time.Since(start) < 50*time.Millisecond {
		t.Fatal("the send gave up before its timeout")
	}
}

func TestSendKillDeliversToListener(t *testing.T) {
	ch := make(chan bool)
	got := make(chan bool, 1)
	go func() { got <- <-ch }()
	if !SendKill(ch, time.Second) {
		t.Fatal("a waiting listener must take the signal")
	}
	if !<-got {
		t.Fatal("the listener must receive true")
	}
}
