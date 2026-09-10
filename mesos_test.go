package traefik_mesos_plugin

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/traefik/genconf/dynamic"
)

func TestPluginLifecyclePublishesMesosConfiguration(t *testing.T) {
	agent := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`[{"executor_id":"task.1"}]`))
	}))
	defer agent.Close()
	host, portText, err := net.SplitHostPort(agent.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		t.Fatal(err)
	}

	master := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/tasks":
			_, _ = w.Write([]byte(`{"tasks":[{"id":"task.1","name":"Web App","slave_id":"agent-1","state":"TASK_RUNNING","labels":[{"key":"traefik.http.routers.web.rule","value":"Host(` + "`" + `web.example` + "`" + `)"},{"key":"traefik.http.routers.web.service","value":"web"},{"key":"traefik.http.services.web.loadbalancer.server.port","value":"8080"}],"statuses":[{"state":"TASK_STARTING","container_status":{"network_infos":[{"ip_addresses":[{"protocol":"IPv4","ip_address":"192.0.2.20"}]}]}},{"state":"TASK_RUNNING","container_status":{"network_infos":[{"ip_addresses":[{"protocol":"IPv4","ip_address":"192.0.2.10"}]}]}}]}]}`))
		case "/slaves/":
			_, _ = w.Write([]byte(`{"slaves":[{"id":"agent-1","hostname":"` + host + `","port":` + strconv.Itoa(port) + `}]}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer master.Close()

	p, err := New(context.Background(), &Config{Endpoint: master.URL, PollInterval: "1ms", PollTimeout: "1s"}, "test")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = p.Stop() }()
	if err := p.Init(); err != nil {
		t.Fatal(err)
	}
	ch := make(chan json.Marshaler, 1)
	if err := p.Provide(ch); err != nil {
		t.Fatal(err)
	}
	select {
	case payload := <-ch:
		raw, err := payload.MarshalJSON()
		if err != nil {
			t.Fatal(err)
		}
		var got dynamic.Configuration
		if err := json.Unmarshal(raw, &got); err != nil {
			t.Fatal(err)
		}
		if got.HTTP.Routers["web"].Rule != "Host(`web.example`)" {
			t.Fatalf("unexpected router: %#v", got.HTTP.Routers["web"])
		}
		if got.HTTP.Services["web"].LoadBalancer.Servers[0].URL != "http://192.0.2.20:8080" {
			t.Fatalf("unexpected backend: %#v", got.HTTP.Services["web"])
		}
	case <-time.After(2 * time.Second):
		t.Fatal("provider did not publish configuration")
	}
}

func TestNewRejectsInvalidDuration(t *testing.T) {
	if _, err := New(context.Background(), &Config{PollInterval: "invalid"}, "test"); err == nil {
		t.Fatal("expected invalid duration error")
	}
}

func TestStopIsIdempotent(t *testing.T) {
	p, err := New(context.Background(), &Config{Endpoint: "127.0.0.1:1", PollInterval: "1h"}, "test")
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Stop(); err != nil {
		t.Fatal(err)
	}
	if err := p.Stop(); err != nil {
		t.Fatal(err)
	}
}
