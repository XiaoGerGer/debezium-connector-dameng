# Dameng Kafka Connect image

Run `bash docker/build-image.sh --help` from the repository root for build, image export and base-image options. The script resolves the connector version from the standalone POM, builds the plugin, validates the image configuration and optionally exports an archive with a checksum.

The image includes JMX Exporter 1.4.0 and the metric mapping, exposing `9404/metrics`. Setting `KAFKA_OPTS` replaces the default; retain the `-javaagent` option when enabling monitoring.

See the [connector manual](../documentation/dameng.adoc#dameng-deployment) for deployment, REST configuration and transaction spill storage, and [monitoring](../documentation/dameng.adoc#dameng-monitoring) for Prometheus and Grafana.

The default base is `apache/kafka:3.9.1`, retained from the original deployment tooling. A Dockerfile default is not evidence that the current connector has been validated on that runtime; consult the current build and regression records before deployment, or pass `--base-image` for your validated worker image.
