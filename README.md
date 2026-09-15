# ClusterD/Apache Mesos Provider Plugin for Traefik

This repository contains a standalone [Traefik provider plugin](https://doc.traefik.io/traefik/extend/plugins/plugin-providers/) that discovers running ClusterD/Apache Mesos tasks and converts their `traefik.*` labels into HTTP, TCP, and UDP configuration.


## Configuration

Declare the plugin in Traefik's static configuration:

```yaml
experimental:
  plugins:
    mesos:
      moduleName: github.com/m3scluster/traefik-mesos-plugin
      version: v0.1.1

providers:
  plugin:
    mesos:
      endpoint: mesos-master.example.invalid:5050
      principal: mesos
      secret: ${MESOS_SECRET}
      ssl: false
      pollInterval: 10s
      pollTimeout: 10s
      forceUpdateInterval: 10m
      defaultRule: Host(`{{ normalize .Name }}`)
```

The same configuration as TOML:

```toml
[experimental.plugins.mesos]
  moduleName = "github.com/m3scluster/traefik-mesos-plugin"
  version = "v0.1.1"

[providers.plugin.mesos]
  endpoint = "mesos-master.example.invalid:5050"
  principal = "mesos"
  secret = "${MESOS_SECRET}"
  ssl = false
  pollInterval = "10s"
  pollTimeout = "10s"
  forceUpdateInterval = "10m"
  defaultRule = "Host(`{{ normalize .Name }}`)"
```

For local development, place this repository at `plugins-local/src/github.com/m3scluster/traefik-mesos-plugin` and use:

```yaml
experimental:
  localPlugins:
    mesos:
      moduleName: github.com/m3scluster/traefik-mesos-plugin
providers:
  plugin:
    mesos:
      endpoint: 127.0.0.1:5050
      pollInterval: 2s
```

The Local-Plugin-Modus as TOML:

```toml
[experimental.localPlugins.mesos]
  moduleName = "github.com/m3scluster/traefik-mesos-plugin"

[providers.plugin.mesos]
  endpoint = "127.0.0.1:5050"
  pollInterval = "2s"
```

`endpoint` may include a scheme; otherwise `ssl: true` selects HTTPS. ClusterD/Apache Mesos master and agent requests use HTTP Basic Authentication. The provider queries `/tasks?order=asc&limit=-1`, `/slaves/`, and each agent's `/containers/` endpoint. Only running tasks with at least one `traefik.` label and a matching agent container are published.

Supported fields are `endpoint` (default `127.0.0.1:5050`), `ssl` (default `false`), `principal`, `secret`, `pollInterval` (default `10s`), `pollTimeout` (default `10s`), `forceUpdateInterval` (default `10m`), and `defaultRule`.

The placeholders `__mesos_taskid__` and `__mesos_portname__` are replaced in label keys and values. HTTP, TCP, and UDP backend addresses are built from Mesos task network status and discovery ports.

## Label examples

The following labels can be placed on a ClusterD/Apache Mesos task. The provider reads the labels, finds the matching Mesos discovery port, and publishes the resulting configuration to Traefik.

### HTTP

```text
traefik.enable = true
traefik.http.routers.__mesos_taskid__.rule = Host(`app.example.com`)
traefik.http.routers.__mesos_taskid__.entrypoints = web
traefik.http.routers.__mesos_taskid__.service = __mesos_portname__
traefik.http.routers.__mesos_taskid__.tls = true
traefik.http.routers.__mesos_taskid__.tls.certresolver = letsencrypt
```

For a task with a discovery port named `web`, the provider creates a service for that port. An explicit backend port can also be supplied:

```text
traefik.http.routers.__mesos_taskid__.rule = Host(`app.example.com`)
traefik.http.routers.__mesos_taskid__.service = app
traefik.http.services.app.loadbalancer.server.port = 8080
```

### TCP

```text
traefik.tcp.routers.__mesos_taskid__.rule = HostSNI(`*`)
traefik.tcp.routers.__mesos_taskid__.entrypoints = tcp
traefik.tcp.routers.__mesos_taskid__.service = database
traefik.tcp.services.database.loadbalancer.server.port = 5432
```

### UDP

```text
traefik.udp.routers.__mesos_taskid__.entrypoints = dns
traefik.udp.routers.__mesos_taskid__.service = dns
traefik.udp.services.dns.loadbalancer.server.port = 5353
```

The task ID placeholder is replaced in both label keys and values, with dots changed to underscores. The port-name placeholder uses the first Mesos discovery port name. If no explicit `loadbalancer.server.port` label is present, the provider uses the matching discovery port number.

## Development

Dependencies are vendored because Traefik plugins load vendored source. Run:

```sh
make check
make vendor
```

`make check` runs formatting validation, unit tests, `go vet`, and a compile build. `golangci-lint` is available through the optional `make lint` target when installed.
