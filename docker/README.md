# Dameng Kafka Connect image

Run `bash docker/build-custom-image.sh --help` from the repository root for build, image export and base-image options. To build from an existing plugin archive:

```bash
bash docker/build-custom-image.sh \
  --plugin /path/to/debezium-connector-dameng-3.8.0-SNAPSHOT-kafka2-compatible-plugin.tar.gz \
  --image debezium-connect-dameng:custom \
  --output /path/to/debezium-connect-dameng-custom.tar.gz
```

`--plugin` accepts an absolute path or a path relative to your working directory, including files outside the repository. Quote paths containing spaces. This mode does not invoke Java or Maven; do not append Maven options. The archive must contain the plugin directory and its runtime dependencies, as produced by the assembly build. It is copied into a temporary Docker context that is removed when the script exits.

For `debezium-connector-dameng-<version>-plugin.tar.gz`, the default image tag, image version label and export filename use `<version>`, including any compatibility suffix. Other archive names use `custom`. `--image` changes the image name/tag; `--output` independently changes the export path. By default the export is `target/debezium-connect-dameng-<version>-<platform>.tar.gz` with a `.sha256` sidecar. Use `--no-export` to keep only the local image.

Without `--plugin`, the script resolves the connector version from the standalone POM and builds the plugin with Maven. `--skip-build` reuses the local assembly archive but still invokes Maven to resolve its name. Both modes validate the image's environment configuration before exporting it. The Docker daemon is required; uncached base images and a missing JMX agent require downloads.

The image includes JMX Exporter 1.4.0 and the metric mapping, exposing `9404/metrics`. Setting `KAFKA_OPTS` replaces the default; retain the `-javaagent` option when enabling monitoring.

Set `LOG_LEVEL=DEBUG` to enable global DEBUG logging to the container console, with no logging configuration mount required:

```yaml
environment:
  LOG_LEVEL: "DEBUG"
```

The entrypoint accepts `ALL`, `TRACE`, `DEBUG`, `INFO`, `WARN`, `ERROR`, `FATAL` and `OFF` (case insensitive). Unset or empty values retain the base image's logging defaults. Explicit `KAFKA_LOG4J_OPTS` takes precedence and produces a notice that `LOG_LEVEL` is ignored. This convenience option targets the default Kafka 3.9.1 logging runtime and preserves `KAFKA_OPTS`, including the JMX agent. Rebuild the image to include the updated entrypoint and recreate the container to apply the environment variable; use the Connect logging REST API for changes to a running worker.

See the [connector manual](../documentation/dameng.adoc#dameng-deployment) for deployment, REST configuration and transaction spill storage, and [monitoring](../documentation/dameng.adoc#dameng-monitoring) for Prometheus and Grafana.

The default base is `apache/kafka:3.9.1`, retained from the original deployment tooling. A Dockerfile default is not evidence that the current connector has been validated on that runtime; consult the current build and regression records before deployment, or pass `--base-image` for your validated worker image.
