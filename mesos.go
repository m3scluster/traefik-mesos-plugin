package traefik_mesos_plugin

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"text/template"
	"time"

	"github.com/traefik/genconf/dynamic"
)

const defaultRule = "Host(`{{ normalize .Name }}`)"

type Config struct {
	Endpoint            string `json:"endpoint,omitempty"`
	SSL                 bool   `json:"ssl,omitempty"`
	Principal           string `json:"principal,omitempty"`
	Secret              string `json:"secret,omitempty"`
	PollInterval        string `json:"pollInterval,omitempty"`
	PollTimeout         string `json:"pollTimeout,omitempty"`
	ForceUpdateInterval string `json:"forceUpdateInterval,omitempty"`
	DefaultRule         string `json:"defaultRule,omitempty"`
}

type jsonPayload []byte

func (p jsonPayload) MarshalJSON() ([]byte, error) { return []byte(p), nil }

func CreateConfig() *Config {
	return &Config{Endpoint: "127.0.0.1:5050", PollInterval: "10s", PollTimeout: "10s", ForceUpdateInterval: "10m", DefaultRule: defaultRule}
}

type Provider struct {
	endpoint, principal, secret, defaultRule string
	ssl                                      bool
	pollInterval, pollTimeout, forceUpdate   time.Duration
	ctx                                      context.Context
	cancel                                   context.CancelFunc
	client                                   *http.Client
	mu                                       sync.Mutex
	lastHash                                 string
	lastUpdate                               time.Time
}

func New(ctx context.Context, config *Config, _ string) (*Provider, error) {
	if config == nil {
		config = CreateConfig()
	}
	parse := func(value, field string, fallback time.Duration) (time.Duration, error) {
		if value == "" {
			return fallback, nil
		}
		d, err := time.ParseDuration(value)
		if err != nil {
			return 0, fmt.Errorf("invalid %s: %w", field, err)
		}
		return d, nil
	}
	pi, err := parse(config.PollInterval, "pollInterval", 10*time.Second)
	if err != nil {
		return nil, err
	}
	pt, err := parse(config.PollTimeout, "pollTimeout", 10*time.Second)
	if err != nil {
		return nil, err
	}
	fu, err := parse(config.ForceUpdateInterval, "forceUpdateInterval", 10*time.Minute)
	if err != nil {
		return nil, err
	}
	if pi <= 0 || pt <= 0 || fu <= 0 {
		return nil, fmt.Errorf("poll intervals must be greater than zero")
	}
	endpoint := config.Endpoint
	if endpoint == "" {
		endpoint = "127.0.0.1:5050"
	}
	scheme := "http"
	if config.SSL {
		scheme = "https"
	}
	if !strings.Contains(endpoint, "://") {
		endpoint = scheme + "://" + endpoint
	}
	if config.DefaultRule == "" {
		config.DefaultRule = defaultRule
	}
	if _, err := template.New("rule").Funcs(template.FuncMap{"normalize": func(value string) string { return strings.ToLower(strings.ReplaceAll(value, " ", "-")) }}).Parse(config.DefaultRule); err != nil {
		return nil, fmt.Errorf("invalid defaultRule: %w", err)
	}
	child, cancel := context.WithCancel(ctx)
	return &Provider{endpoint: strings.TrimRight(endpoint, "/"), ssl: config.SSL, principal: config.Principal, secret: config.Secret, defaultRule: config.DefaultRule, pollInterval: pi, pollTimeout: pt, forceUpdate: fu, ctx: child, cancel: cancel, client: &http.Client{Timeout: pt, Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}}}, nil
}

func (p *Provider) Init() error { return nil }
func (p *Provider) Stop() error {
	if p.cancel != nil {
		p.cancel()
	}
	return nil
}
func (p *Provider) Provide(ch chan<- json.Marshaler) error { go p.run(ch); return nil }

func (p *Provider) run(ch chan<- json.Marshaler) {
	ticker := time.NewTicker(p.pollInterval)
	defer ticker.Stop()
	for {
		if err := p.publish(ch); err != nil {
			select {
			case <-time.After(time.Second):
			case <-p.ctx.Done():
				return
			}
		}
		select {
		case <-ticker.C:
		case <-p.ctx.Done():
			return
		}
	}
}
func (p *Provider) publish(ch chan<- json.Marshaler) error {
	tasks, err := p.fetchTasks()
	if err != nil {
		return err
	}
	conf, err := p.configuration(tasks)
	if err != nil {
		return err
	}
	data, err := json.Marshal(conf)
	if err != nil {
		return err
	}
	hash := fmt.Sprintf("%x", data)
	p.mu.Lock()
	defer p.mu.Unlock()
	if hash == p.lastHash && time.Since(p.lastUpdate) < p.forceUpdate {
		return nil
	}
	ch <- jsonPayload(data)
	p.lastHash, p.lastUpdate = hash, time.Now()
	return nil
}

type tasksResponse struct {
	Tasks []task `json:"tasks"`
}
type task struct {
	ID        string   `json:"id"`
	Name      string   `json:"name"`
	SlaveID   string   `json:"slave_id"`
	State     string   `json:"state"`
	Labels    []label  `json:"labels"`
	Statuses  []status `json:"statuses"`
	Discovery struct {
		Ports struct {
			Ports []port `json:"ports"`
		} `json:"ports"`
	} `json:"discovery"`
}
type label struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}
type port struct {
	Number   int    `json:"number"`
	Name     string `json:"name"`
	Protocol string `json:"protocol"`
}
type status struct {
	State           string `json:"state"`
	ContainerStatus struct {
		NetworkInfos []struct {
			IPAddresses []struct {
				Protocol  string `json:"protocol"`
				IPAddress string `json:"ip_address"`
			} `json:"ip_addresses"`
		} `json:"network_infos"`
	} `json:"container_status"`
}
type agentsResponse struct {
	Slaves []struct {
		ID       string `json:"id"`
		Hostname string `json:"hostname"`
		Port     int    `json:"port"`
	} `json:"slaves"`
}
type agentContainer struct {
	ExecutorID string `json:"executor_id"`
}

func (p *Provider) request(path string, target any) error {
	req, err := http.NewRequestWithContext(p.ctx, http.MethodGet, p.endpoint+path, nil)
	if err != nil {
		return err
	}
	req.SetBasicAuth(p.principal, p.secret)
	res, err := p.client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("Mesos returned HTTP %d for %s", res.StatusCode, path)
	}
	return json.NewDecoder(res.Body).Decode(target)
}
func (p *Provider) fetchTasks() ([]task, error) {
	var r tasksResponse
	if err := p.request("/tasks?order=asc&limit=-1", &r); err != nil {
		return nil, err
	}
	var a agentsResponse
	if err := p.request("/slaves/", &a); err != nil {
		return nil, err
	}
	agents := map[string]string{}
	for _, x := range a.Slaves {
		agents[x.ID] = net.JoinHostPort(x.Hostname, strconv.Itoa(x.Port))
	}
	out := make([]task, 0)
	for _, t := range r.Tasks {
		if t.State != "TASK_RUNNING" || !hasTraefikLabel(t.Labels) {
			continue
		}
		host, ok := agents[t.SlaveID]
		if !ok {
			continue
		}
		var cs []agentContainer
		req, err := http.NewRequestWithContext(p.ctx, http.MethodGet, p.agentURL(host)+"/containers/", nil)
		if err != nil {
			return nil, err
		}
		req.SetBasicAuth(p.principal, p.secret)
		res, err := p.client.Do(req)
		if err != nil {
			return nil, err
		}
		err = json.NewDecoder(res.Body).Decode(&cs)
		res.Body.Close()
		if err != nil {
			return nil, err
		}
		for _, c := range cs {
			if c.ExecutorID == t.ID {
				out = append(out, t)
				break
			}
		}
	}
	return out, nil
}
func (p *Provider) agentURL(host string) string {
	scheme := "http"
	if p.ssl {
		scheme = "https"
	}
	return scheme + "://" + host
}
func hasTraefikLabel(ls []label) bool {
	for _, l := range ls {
		if strings.HasPrefix(strings.ToLower(l.Key), "traefik.") {
			return true
		}
	}
	return false
}

func (p *Provider) configuration(tasks []task) (*dynamic.Configuration, error) {
	c := &dynamic.Configuration{HTTP: &dynamic.HTTPConfiguration{Routers: map[string]*dynamic.Router{}, Services: map[string]*dynamic.Service{}, Middlewares: map[string]*dynamic.Middleware{}, ServersTransports: map[string]*dynamic.ServersTransport{}}, TCP: &dynamic.TCPConfiguration{Routers: map[string]*dynamic.TCPRouter{}, Services: map[string]*dynamic.TCPService{}}, UDP: &dynamic.UDPConfiguration{Routers: map[string]*dynamic.UDPRouter{}, Services: map[string]*dynamic.UDPService{}}}
	for _, t := range tasks {
		labels := map[string]string{}
		portName := ""
		if len(t.Discovery.Ports.Ports) > 0 {
			portName = t.Discovery.Ports.Ports[0].Name
		}
		for _, l := range t.Labels {
			taskID := strings.ReplaceAll(t.ID, ".", "_")
			k := strings.ReplaceAll(strings.ReplaceAll(l.Key, "__mesos_taskid__", taskID), "__mesos_portname__", portName)
			v := strings.ReplaceAll(strings.ReplaceAll(l.Value, "__mesos_taskid__", taskID), "__mesos_portname__", portName)
			labels[k] = v
		}
		p.applyLabels(c, t, labels)
		p.fillDiscoveredServers(c, t)
	}
	return c, nil
}

func (p *Provider) fillDiscoveredServers(c *dynamic.Configuration, t task) {
	for name, service := range c.HTTP.Services {
		if service.LoadBalancer == nil || len(service.LoadBalancer.Servers) > 0 {
			continue
		}
		for _, port := range t.Discovery.Ports.Ports {
			if port.Name != name || port.Protocol == "udp" {
				continue
			}
			scheme := "http"
			if port.Protocol == "https" || port.Protocol == "h2c" || port.Protocol == "wss" {
				scheme = port.Protocol
			}
			service.LoadBalancer.Servers = p.servers(t, strconv.Itoa(port.Number), scheme, nil)
			break
		}
	}
	for name, service := range c.TCP.Services {
		if service.LoadBalancer == nil || len(service.LoadBalancer.Servers) > 0 {
			continue
		}
		for _, port := range t.Discovery.Ports.Ports {
			if port.Name == name && port.Protocol == "tcp" {
				service.LoadBalancer.Servers = p.tcpServers(t, strconv.Itoa(port.Number), "tcp")
				break
			}
		}
	}
	for name, service := range c.UDP.Services {
		if service.LoadBalancer == nil || len(service.LoadBalancer.Servers) > 0 {
			continue
		}
		for _, port := range t.Discovery.Ports.Ports {
			if port.Name == name && port.Protocol == "udp" {
				service.LoadBalancer.Servers = p.udpServers(t, strconv.Itoa(port.Number), "udp")
				break
			}
		}
	}
}

func (p *Provider) applyLabels(c *dynamic.Configuration, t task, labels map[string]string) {
	for key, value := range labels {
		parts := strings.Split(key, ".")
		if len(parts) < 5 || parts[0] != "traefik" {
			continue
		}
		kind, object, name := parts[1], parts[2], parts[3]
		field := strings.Join(parts[4:], ".")
		switch kind {
		case "http":
			p.httpLabel(c.HTTP, t, object, name, field, value)
		case "tcp":
			p.tcpLabel(c.TCP, t, object, name, field, value)
		case "udp":
			p.udpLabel(c.UDP, t, object, name, field, value)
		}
	}
}
func csv(v string) []string {
	var out []string
	for _, x := range strings.Split(v, ",") {
		if strings.TrimSpace(x) != "" {
			out = append(out, strings.TrimSpace(x))
		}
	}
	return out
}
func (p *Provider) httpLabel(c *dynamic.HTTPConfiguration, t task, object, n, f, v string) {
	if object == "services" {
		if c.Services[n] == nil {
			c.Services[n] = &dynamic.Service{LoadBalancer: &dynamic.ServersLoadBalancer{}}
		}
		if f == "loadbalancer.server.port" {
			c.Services[n].LoadBalancer.Servers = p.servers(t, v, "http", nil)
		}
		return
	}
	r := c.Routers[n]
	if r == nil {
		r = &dynamic.Router{Service: n}
		c.Routers[n] = r
	}
	switch f {
	case "rule":
		r.Rule = v
	case "service":
		r.Service = v
	case "entrypoints":
		r.EntryPoints = csv(v)
	case "middlewares":
		r.Middlewares = csv(v)
	case "priority":
		r.Priority, _ = strconv.Atoi(v)
	case "tls":
		if strings.EqualFold(v, "true") {
			r.TLS = &dynamic.RouterTLSConfig{}
		}
	case "tls.certresolver":
		if r.TLS == nil {
			r.TLS = &dynamic.RouterTLSConfig{}
		}
		r.TLS.CertResolver = v
	case "tls.options":
		if r.TLS == nil {
			r.TLS = &dynamic.RouterTLSConfig{}
		}
		r.TLS.Options = v
	}
	if r.Rule == "" {
		r.Rule = p.defaultRuleFor(t)
	}
	if c.Services[r.Service] == nil {
		c.Services[r.Service] = &dynamic.Service{LoadBalancer: &dynamic.ServersLoadBalancer{}}
	}
}
func (p *Provider) tcpLabel(c *dynamic.TCPConfiguration, t task, object, n, f, v string) {
	if object == "services" {
		if c.Services[n] == nil {
			c.Services[n] = &dynamic.TCPService{LoadBalancer: &dynamic.TCPServersLoadBalancer{}}
		}
		if f == "loadbalancer.server.port" {
			c.Services[n].LoadBalancer.Servers = p.tcpServers(t, v, "tcp")
		}
		return
	}
	r := c.Routers[n]
	if r == nil {
		r = &dynamic.TCPRouter{Service: n}
		c.Routers[n] = r
	}
	switch f {
	case "rule":
		r.Rule = v
	case "service":
		r.Service = v
	case "entrypoints":
		r.EntryPoints = csv(v)
	case "tls":
		if strings.EqualFold(v, "true") {
			r.TLS = &dynamic.RouterTCPTLSConfig{}
		}
	case "tls.certresolver":
		if r.TLS == nil {
			r.TLS = &dynamic.RouterTCPTLSConfig{}
		}
		r.TLS.CertResolver = v
	case "tls.options":
		if r.TLS == nil {
			r.TLS = &dynamic.RouterTCPTLSConfig{}
		}
		r.TLS.Options = v
	}
	if c.Services[r.Service] == nil {
		c.Services[r.Service] = &dynamic.TCPService{LoadBalancer: &dynamic.TCPServersLoadBalancer{}}
	}
}
func (p *Provider) udpLabel(c *dynamic.UDPConfiguration, t task, object, n, f, v string) {
	if object == "services" {
		if c.Services[n] == nil {
			c.Services[n] = &dynamic.UDPService{LoadBalancer: &dynamic.UDPServersLoadBalancer{}}
		}
		if f == "loadbalancer.server.port" {
			c.Services[n].LoadBalancer.Servers = p.udpServers(t, v, "udp")
		}
		return
	}
	r := c.Routers[n]
	if r == nil {
		r = &dynamic.UDPRouter{Service: n}
		c.Routers[n] = r
	}
	switch f {
	case "service":
		r.Service = v
	case "entrypoints":
		r.EntryPoints = csv(v)
	}
	if c.Services[r.Service] == nil {
		c.Services[r.Service] = &dynamic.UDPService{LoadBalancer: &dynamic.UDPServersLoadBalancer{}}
	}
}
func (p *Provider) defaultRuleFor(t task) string {
	return strings.ReplaceAll(strings.ReplaceAll(p.defaultRule, "{{ normalize .Name }}", strings.ToLower(strings.ReplaceAll(t.Name, " ", "-"))), "{{.Name}}", t.Name)
}
func (p *Provider) ips(t task) []string {
	var out []string
	statuses := t.Statuses
	for i := len(t.Statuses) - 1; i >= 0; i-- {
		if t.Statuses[i].State == "TASK_STARTING" {
			statuses = t.Statuses[i : i+1]
			break
		}
	}
	if len(statuses) == len(t.Statuses) {
		for i := len(t.Statuses) - 1; i >= 0; i-- {
			if t.Statuses[i].State == "TASK_RUNNING" {
				statuses = t.Statuses[i : i+1]
				break
			}
		}
	}
	for _, s := range statuses {
		for _, n := range s.ContainerStatus.NetworkInfos {
			for _, ip := range n.IPAddresses {
				if ip.Protocol == "IPv4" {
					out = append(out, ip.IPAddress)
				}
			}
		}
	}
	return out
}
func (p *Provider) servers(t task, port, scheme string, _ []dynamic.Server) []dynamic.Server {
	var out []dynamic.Server
	for _, ip := range p.ips(t) {
		out = append(out, dynamic.Server{URL: scheme + "://" + net.JoinHostPort(ip, port)})
	}
	return out
}
func (p *Provider) tcpServers(t task, port, _ string) []dynamic.TCPServer {
	var out []dynamic.TCPServer
	for _, ip := range p.ips(t) {
		out = append(out, dynamic.TCPServer{Address: net.JoinHostPort(ip, port)})
	}
	return out
}
func (p *Provider) udpServers(t task, port, _ string) []dynamic.UDPServer {
	var out []dynamic.UDPServer
	for _, ip := range p.ips(t) {
		out = append(out, dynamic.UDPServer{Address: net.JoinHostPort(ip, port)})
	}
	return out
}
