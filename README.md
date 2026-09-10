# ClusterD/Apache Mesos Provider Plugin for Traefik

This repository contains a standalone [Traefik provider plugin](https://doc.traefik.io/traefik/extend/plugins/plugin-providers/) that discovers running ClusterD/Apache Mesos tasks and converts their `traefik.*` labels into HTTP, TCP, and UDP configuration.

The plugin uses the [ClusterD](https://www.clusterd.de/) logo as its catalog icon (`assets/clusterd-mark.png`) and combines ClusterD/Apache Mesos with Traefik in its catalog banner (`assets/clusterd-traefik-banner.png`).

## Configuration

Declare the plugin in Traefik's static configuration:

```yaml
experimental:
  plugins:
    mesos:
      moduleName: github.com/m3scluster/traefik-mesos-plugin
      version: v0.1.0

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

Dieselbe Konfiguration als TOML:

```toml
[experimental.plugins.mesos]
  moduleName = "github.com/m3scluster/traefik-mesos-plugin"
  version = "v0.1.0"

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

Der Local-Plugin-Modus als TOML:

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

## Development

Dependencies are vendored because Traefik plugins load vendored source. Run:

```sh
make check
make vendor
```

`make check` runs formatting validation, unit tests, `go vet`, and a compile build. `golangci-lint` is available through the optional `make lint` target when installed.
