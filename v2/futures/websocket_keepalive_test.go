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
