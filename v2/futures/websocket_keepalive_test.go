package futures

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// A stream the server never writes to must still carry client PINGs, so a network path that
// drops connections idle for some seconds never sees it idle.
func TestWsServeSendsClientPings(t *testing.T) {
	const interval, hold, want = 20 * time.Millisecond, 150 * time.Millisecond, 3
	origInterval := WebsocketPingInterval
	WebsocketPingInterval = interval
	defer func() { WebsocketPingInterval = origInterval }()

	var pings int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		conn.SetPingHandler(func(string) error { atomic.AddInt32(&pings, 1); return nil })
		for { // control frames are processed inside reads; this server never sends data
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	defer srv.Close()

	doneC, stopC, err := wsServe(&WsConfig{Endpoint: "ws" + strings.TrimPrefix(srv.URL, "http")}, func([]byte) {}, func(error) {})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(hold)
	close(stopC)
	<-doneC
	if n := atomic.LoadInt32(&pings); n < want {
		t.Fatalf("client sent %d PINGs in %v at a %v interval, want >= %d", n, hold, interval, want)
	}
}

// keepAlive's liveness check: server PINGs keep the connection open, and once they stop it closes
// after about `timeout`. The PING handler (read goroutine) and the checker (ticker goroutine) share
// the last-response time, so this is also a race test: run it with -race.
func TestKeepAliveTracksServerPings(t *testing.T) {
	const timeout, pingEvery, pingFor = 100 * time.Millisecond, 10 * time.Millisecond, 400 * time.Millisecond
	origTimeout, origInterval := WebsocketTimeout, WebsocketPingInterval
	WebsocketTimeout, WebsocketPingInterval = timeout, time.Hour // client PINGs out of the way
	defer func() { WebsocketTimeout, WebsocketPingInterval = origTimeout, origInterval }()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		clientGone := make(chan struct{})
		go func() { // reads process the client's PONGs; an error means the client closed
			defer close(clientGone)
			for {
				if _, _, err := conn.ReadMessage(); err != nil {
					return
				}
			}
		}()
		for end := time.Now().Add(pingFor); time.Now().Before(end); time.Sleep(pingEvery) {
			if conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(time.Second)) != nil {
				return
			}
		}
		<-clientGone // then fall silent until the client gives up
	}))
	defer srv.Close()

	doneC, _, err := wsServe(&WsConfig{Endpoint: "ws" + strings.TrimPrefix(srv.URL, "http")}, func([]byte) {}, func(error) {})
	if err != nil {
		t.Fatal(err)
	}
	t0 := time.Now()
	select {
	case <-doneC:
		t.Fatalf("closed after %v while the server was still pinging", time.Since(t0))
	case <-time.After(pingFor - 2*timeout):
	}
	select {
	case <-doneC: // the liveness check closed it once the PINGs stopped
	case <-time.After(pingFor + 10*timeout):
		t.Fatal("still open long after the server stopped pinging")
	}
}
