#!/usr/bin/env bash
set -euo pipefail

if [[ -v HEAP_OPTS ]]; then
    export KAFKA_HEAP_OPTS="$HEAP_OPTS"
fi

if [[ -n ${LOG_LEVEL:-} ]]; then
    log_level=${LOG_LEVEL^^}
    case "$log_level" in
        ALL|TRACE|DEBUG|INFO|WARN|ERROR|FATAL|OFF) ;;
        *) printf 'Invalid LOG_LEVEL: %s (use ALL, TRACE, DEBUG, INFO, WARN, ERROR, FATAL or OFF)\n' "$LOG_LEVEL" >&2; exit 2 ;;
    esac
    if [[ -n ${KAFKA_LOG4J_OPTS:-} ]]; then
        printf 'KAFKA_LOG4J_OPTS is set; LOG_LEVEL is ignored in favor of the custom logging configuration.\n' >&2
    else
        # The default Kafka 3.9.1 image uses Log4j-compatible properties.
        log_config=$(mktemp /tmp/dameng-connect-log4j.XXXXXX.properties)
        cat > "$log_config" <<EOF
log4j.rootLogger=$log_level, stdout
log4j.appender.stdout=org.apache.log4j.ConsoleAppender
log4j.appender.stdout.layout=org.apache.log4j.PatternLayout
log4j.appender.stdout.layout.ConversionPattern=[%d] %-5p %X{connector.context}%m (%c)%n
EOF
        export KAFKA_LOG4J_OPTS="-Dlog4j.configuration=file:$log_config"
    fi
fi

worker_config=$(mktemp /tmp/dameng-connect.XXXXXX.properties)
/opt/kafka/bin/configure-connect.sh /etc/kafka/connect-distributed.properties "$worker_config"
exec /opt/kafka/bin/connect-distributed.sh "$worker_config"
