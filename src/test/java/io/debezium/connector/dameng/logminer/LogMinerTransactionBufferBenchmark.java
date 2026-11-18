/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng.logminer;

import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.slf4j.LoggerFactory;

import io.debezium.connector.dameng.DamengConnectorConfig.LogMiningBufferType;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;

/** Standalone, opt-in benchmark. Uses real spill files; never starts a database or a connector. */
public final class LogMinerTransactionBufferBenchmark {
    private static volatile long metricsSink;

    private LogMinerTransactionBufferBenchmark() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2 || args.length > 4) {
            throw new IllegalArgumentException("Usage: <memory|hybrid-small|hybrid-skewed|spilled-append> <transactions> [warmups=1] [runs=3]");
        }
        final String scenario = args[0];
        final int count = Integer.parseInt(args[1]);
        final int warmups = args.length > 2 ? Integer.parseInt(args[2]) : 1;
        final int runs = args.length > 3 ? Integer.parseInt(args[3]) : 3;
        if (count < 1 || warmups < 0 || runs < 1) {
            throw new IllegalArgumentException("Positive transactions/runs and nonnegative warmups required");
        }
        final var logging = (LoggerContext) LoggerFactory.getILoggerFactory();
        logging.getLoggerList().forEach(logger -> logger.setLevel(Level.OFF));
        final Workload workload = workload(scenario, count);
        final Path directory = Files.createTempDirectory("dameng-buffer-benchmark-");
        try {
            System.out.println(
                    "scenario,transactions,run,add_ms,read_release_ms,close_ms,spills,resident_bytes,peak_resident_bytes,disk_bytes,allocated_bytes,rows,checksum");
            for (int i = -warmups; i < runs; i++) {
                run(scenario, count, i, directory, workload);
            }
        }
        finally {
            // Every buffer must have removed its files, even when validation failed.
            Files.delete(directory);
        }
    }

    private static Workload workload(String scenario, int count) {
        final boolean memory = scenario.equals("memory");
        final boolean appends = scenario.equals("spilled-append");
        if (!memory && !appends && !scenario.equals("hybrid-small") && !scenario.equals("hybrid-skewed")) {
            throw new IllegalArgumentException("Unknown scenario: " + scenario);
        }
        final long limit = memory ? count * 2048L + 1048576 : appends ? count * 1200L + 4096 : count * 384L + 1048576;
        final List<LogMinerRow> input = new ArrayList<>();
        final Map<String, List<LogMinerRow>> expected = new LinkedHashMap<>();
        if (appends) {
            if (limit > 16 * 1024 * 1024) {
                throw new IllegalArgumentException("spilled-append workload exceeds its single SQL limit; use fewer transactions");
            }
            data(input, expected, "hot", "h".repeat((int) limit));
        }
        for (int i = 0; i < count; i++) {
            final String id = "tx-" + i;
            final int length = memory || appends ? 128 : scenario.equals("hybrid-skewed") && i % 20 == 0 ? 16384 : 1024;
            data(input, expected, id, id + "x".repeat(length));
        }
        if (appends) {
            for (int i = 0; i < 8; i++) {
                data(input, expected, "hot", i + "y".repeat((int) Math.min(1048576L, limit / 2)));
            }
        }
        for (String id : expected.keySet()) {
            input.add(row(id, input.size() + 1L, 7, null));
        }
        return new Workload(memory ? LogMiningBufferType.MEMORY : LogMiningBufferType.HYBRID, limit, input, expected);
    }

    private static void data(List<LogMinerRow> input, Map<String, List<LogMinerRow>> expected, String id, String sql) {
        final var row = row(id, input.size() + 1L, 1, sql);
        input.add(row);
        expected.computeIfAbsent(id, ignored -> new ArrayList<>()).add(row);
    }

    private static LogMinerRow row(String id, long lsn, int operation, String sql) {
        return new LogMinerRow(lsn, 1, 0, id, operation, 0, null, null, null, sql, "r", 0, 0, Instant.EPOCH, 7);
    }

    private static void run(String scenario, int count, int run, Path directory, Workload workload) throws Exception {
        final var allocation = (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();
        final long thread = Thread.currentThread().getId();
        final long startAllocated = allocation.getThreadAllocatedBytes(thread);
        long peak = 0;
        long spills;
        long resident;
        long disk;
        long allocated;
        long addNanos;
        long readNanos;
        long beforeClose;
        long rowsRead = 0;
        long checksum = 0;
        try (var buffer = new LogMinerTransactionBuffer(workload.limit(), 32 * 1024 * 1024, count + 1L, directory,
                4L * 1024 * 1024 * 1024, 0, workload.type())) {
            buffer.beginWindow();
            final long start = System.nanoTime();
            for (var row : workload.input()) {
                buffer.add(row);
                peak = Math.max(peak, buffer.memoryBytes());
                if ((buffer.windowRecords() & 1023) == 0) {
                    metricsSink = buffer.openTransactions() + buffer.memoryBytes() + buffer.diskBytes() + buffer.bufferedRecords();
                }
            }
            buffer.finishWindow();
            addNanos = System.nanoTime() - start;
            allocated = allocation.getThreadAllocatedBytes(thread) - startAllocated;
            spills = buffer.spilledTransactions();
            resident = buffer.memoryBytes();
            disk = buffer.diskBytes();
            if (buffer.openTransactions() != 0 || resident > workload.limit()) {
                throw new AssertionError("Invalid transaction count or memory estimate");
            }
            final long readStart = System.nanoTime();
            final var committed = buffer.committedTransactions();
            if (committed.size() != workload.expected().size()) {
                throw new AssertionError("Lost transaction");
            }
            for (var tx : committed) {
                try (var cursor = tx.open()) {
                    for (var expected : workload.expected().get(tx.id())) {
                        if (!expected.equals(cursor.next())) {
                            throw new AssertionError("Changed row/order for " + tx.id());
                        }
                        rowsRead++;
                        checksum += expected.lsn() + expected.redo().hashCode();
                    }
                    if (cursor.next() != null) {
                        throw new AssertionError("Extra row for " + tx.id());
                    }
                }
                buffer.release(tx);
            }
            final long end = workload.input().size();
            if (buffer.completeWindow(end) != end || buffer.memoryBytes() != 0 || buffer.diskBytes() != 0 || buffer.bufferedRecords() != 0) {
                throw new AssertionError("Leaked transaction state");
            }
            readNanos = System.nanoTime() - readStart;
            beforeClose = System.nanoTime();
        }
        final long closeNanos = System.nanoTime() - beforeClose;
        try (var remaining = Files.list(directory)) {
            if (remaining.findAny().isPresent()) {
                throw new AssertionError("Leaked spill storage");
            }
        }
        if (run >= 0) {
            System.out.printf(Locale.ROOT, "%s,%d,%d,%.3f,%.3f,%.3f,%d,%d,%d,%d,%d,%d,%d%n", scenario, count, run,
                    addNanos / 1e6, readNanos / 1e6, closeNanos / 1e6, spills, resident, peak, disk, allocated, rowsRead, checksum);
        }
    }

    private record Workload(LogMiningBufferType type, long limit, List<LogMinerRow> input, Map<String, List<LogMinerRow>> expected) {
    }
}
