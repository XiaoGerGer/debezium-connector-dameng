# Dameng Connect monitoring

The [Grafana dashboard](dameng-cdc-dashboard.json) and [provisioning configuration](grafana-provider.yaml) are maintained here together with the exporter configuration, Prometheus recording rules, Kubernetes examples and rule tests.

See the [monitoring manual](../../documentation/dameng.adoc#dameng-monitoring) for installation, metric interpretation and validation commands. When adding metrics, update the connector, exporter, recording rules and dashboard together. The dashboard includes a dedicated incremental snapshot section.
