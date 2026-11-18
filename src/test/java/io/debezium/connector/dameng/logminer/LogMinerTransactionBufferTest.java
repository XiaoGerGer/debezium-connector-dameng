/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng.logminer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import io.debezium.connector.dameng.DamengConnectorConfig.LogMiningBufferType;
import io.debezium.connector.dameng.logminer.LogMinerTransactionBuffer.DispatchProgress;
import io.debezium.connector.dameng.logminer.events.EventType;

class LogMinerTransactionBufferTest {
    @TempDir
    Path directory;

    @Test
    void shouldRetainAndReleaseExactlyTheMemoryBudgetWithoutCreatingSpillStorage() throws Exception {
        final var unusedPath = directory.resolve("unused");
        // XID "a" metadata: 290 bytes, including eviction links; row with SQL "x", RS_ID "r": 510 bytes.
        try (var buffer = new LogMinerTransactionBuffer(800, 8192, 10, unusedPath, 1, 0, LogMiningBufferType.MEMORY)) {
            buffer.beginWindow();
            buffer.add(row("a", 11, 10, 1, 0, "x"));
            buffer.add(row("a", 12, 10, 7, 0, null));
            buffer.finishWindow();

            assertThat(buffer.memoryBytes()).isEqualTo(800);
            assertThat(buffer.diskBytes()).isZero();
            assertThat(buffer.spilledTransactions()).isZero();
            assertThat(Files.exists(unusedPath)).isFalse();
            final var committed = buffer.committedTransactions();
            assertThat(committed).hasSize(1);
            assertThat(read(committed.get(0))).containsExactly(row("a", 11, 10, 1, 0, "x"));
            buffer.release(committed.get(0));
            assertThat(buffer.completeWindow(20)).isEqualTo(20);
            assertThat(buffer.memoryBytes()).isZero();
            assertThat(buffer.bufferedRecords()).isZero();
        }
        assertEmptyDirectory();
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void shouldPoisonMemoryOnlyBufferOnRowOrMetadataOverflow(boolean metadata) throws Exception {
        final long memory = metadata ? 289 : 800;
        try (var buffer = new LogMinerTransactionBuffer(memory, 8192, 10, directory, 1, 0, LogMiningBufferType.MEMORY)) {
            if (!metadata) {
                buffer.add(row("a", 11, 10, 1, 0, "x"));
            }
            assertThatThrownBy(() -> buffer.add(row("a", 12, 10, metadata ? 6 : 1, 0, metadata ? null : "y")))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("log.mining.buffer.type=memory")
                    .hasMessageContaining("log.mining.buffer.memory.bytes=" + memory);
            assertThat(buffer.isCapacityExceeded()).isTrue();
            assertThat(buffer.memoryBytes()).isLessThanOrEqualTo(memory);
            assertThat(buffer.bufferedRecords()).isEqualTo(metadata ? 0 : 1);
            assertThat(buffer.diskBytes()).isZero();
            assertThat(buffer.spilledTransactions()).isZero();
            assertThatThrownBy(buffer::committedTransactions).hasMessageContaining("log.mining.buffer.type=memory");
            assertThatThrownBy(() -> buffer.completeWindow(20)).hasMessageContaining("log.mining.buffer.type=memory");
            assertThatThrownBy(buffer::resetForReconstruction).hasMessageContaining("log.mining.buffer.type=memory");
            assertThatThrownBy(() -> buffer.add(row("a", 13, 10, 7, 0, null))).hasMessageContaining("log.mining.buffer.type=memory");
            assertEmptyDirectory();
        }
        assertEmptyDirectory();
    }

    @Test
    void shouldTreatOnlyCommitMarkersAsTransactionCompletion() {
        try (var buffer = buffer(4096)) {
            buffer.add(row("open", 10, 10, 6, 0, null));
            buffer.add(row("open", 11, 10, 1, 0, "INSERT open"));
            buffer.add(row("open", 12, 10, 15, 0, null));
            buffer.add(row("xa", 20, 20, 1, 0, "INSERT xa"));
            buffer.add(row("xa", 21, 20, 38, 0, null));

            assertThat(buffer.committedTransactions()).extracting(LogMinerTransactionBuffer.Transaction::id).containsExactly("xa");
            assertThat(buffer.openTransactions()).isEqualTo(1);
            assertThat(buffer.completeWindow(30)).isEqualTo(10);
        }
    }

    @Test
    void shouldIgnoreSequenceAllocationWithoutTransactionIdentity() {
        try (var buffer = buffer(4096)) {
            buffer.add(new LogMinerRow(10, 10, 0, null, 37, 0, null, null, null, "sequence change", "r", 0, 0, Instant.EPOCH));
            buffer.add(row("tx", 11, 10, 1, 0, "INSERT"));
            buffer.add(row("tx", 12, 10, 7, 0, null));

            final var committed = buffer.committedTransactions();
            assertThat(committed).hasSize(1);
            assertThat(read(committed.get(0))).extracting(LogMinerRow::redo).containsExactly("INSERT");
        }
    }

    @Test
    void shouldRejectMissingTransactionIdentity() {
        try (var missingIdentity = buffer(4096)) {
            assertThatThrownBy(() -> missingIdentity.add(row(null, 10, 10, 1, 0, "INSERT")))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("transaction id");
        }
    }

    @ParameterizedTest
    @ValueSource(ints = { Integer.MIN_VALUE, 0, 13, 14, 255, 999, Integer.MAX_VALUE })
    void shouldPreserveUnhandledOperationCodesWhenSpilling(int code) {
        try (var unsupported = buffer(512)) {
            unsupported.add(row("tx", 10, 10, code, 0, "unsupported"));
            unsupported.add(row("tx", 11, 10, 7, 0, null));
            assertThat(unsupported.diskBytes()).isPositive();
            final var rows = read(unsupported.committedTransactions().get(0));
            assertThat(rows).extracting(LogMinerRow::operation).containsExactly(code);
            assertThat(rows).extracting(LogMinerRow::eventType).containsExactly(EventType.from(code));
        }
    }

    @Test
    void shouldReconstructOnlyPostRecoverySuffixOfAnExistingTransaction() {
        try (var buffer = new LogMinerTransactionBuffer(512, 1_000_000, 100, directory, 1_000_000, 100)) {
            assertThat(buffer.completeWindow(110)).isEqualTo(110);
            buffer.beginWindow();
            buffer.add(row("old", 115, 0, 1, 0, "keep"));
            buffer.add(row("old", 116, 0, 1, 0, "rollback"));
            assertThat(buffer.completeWindow(120)).isEqualTo(115);
            buffer.beginWindow();
            buffer.add(row("old", 125, 0, 7, 0, null));
            assertThat(buffer.requiresReconstruction()).isTrue();
            buffer.resetForReconstruction();
            buffer.add(row("old", 115, 50, 1, 0, "keep"));
            buffer.add(row("old", 116, 50, 1, 1, "rollback"));
            buffer.add(row("old", 125, 50, 7, 0, null));
            assertThat(read(buffer.committedTransactions().get(0))).extracting(LogMinerRow::redo).containsExactly("keep");
            buffer.release(buffer.committedTransactions().get(0));
            assertThat(buffer.completeWindow(130)).isEqualTo(130);
        }
    }

    @Test
    void shouldDrainFullRollbackOfARecoverySuffixAndIgnoreUnmatchedTerminalMarkers() {
        try (var buffer = new LogMinerTransactionBuffer(512, 1_000_000, 100, directory, 1_000_000, 100)) {
            buffer.add(row("old", 115, 50, 1, 1, "rollback"));
            buffer.add(row("old", 120, 50, 36, 1, null));
            assertThat(buffer.completeWindow(125)).isEqualTo(125);
            buffer.beginWindow();
            buffer.add(row("unknown", 126, 0, 7, 0, null));
            buffer.add(row("unknown", 127, 0, 38, 0, null));
            buffer.add(row("unknown", 128, 0, 36, 1, null));
            assertThat(buffer.completeWindow(130)).isEqualTo(130);
            assertThat(buffer.memoryBytes()).isZero();
            buffer.add(row("unobserved-at-cut", 135, 0, 1, 0, "post-cut"));
            buffer.add(row("unobserved-at-cut", 136, 0, 7, 0, null));
            assertThat(read(buffer.committedTransactions().get(0))).extracting(LogMinerRow::redo).containsExactly("post-cut");
        }
    }

    @ParameterizedTest
    @ValueSource(longs = { 768, 4096, 1048576 })
    void shouldPreserveConcurrentTransactionsAndReleaseOnlyCompletedHistory(long memory) throws Exception {
        try (var buffer = buffer(memory)) {
            buffer.add(row("long", 11, 10, 1, 0, "first"));
            assertThat(buffer.completeWindow(20)).isEqualTo(10);
            buffer.beginWindow();
            buffer.add(row("short", 22, 21, 1, 0, "short"));
            buffer.add(row("long", 23, 0, 3, 0, "later"));
            buffer.add(row("short", 24, 21, 7, 0, null));
            buffer.finishWindow();
            assertThat(read(buffer.committedTransactions().get(0))).extracting(LogMinerRow::redo).containsExactly("short");
            buffer.release(buffer.committedTransactions().get(0));
            assertThat(buffer.completeWindow(30)).isEqualTo(10);
            buffer.beginWindow();
            buffer.add(row("long", 35, 0, 7, 0, null));
            buffer.finishWindow();
            assertThat(buffer.requiresReconstruction()).isTrue();
            assertThatThrownBy(buffer::committedTransactions).hasMessageContaining("must be reconstructed");
            buffer.resetForReconstruction();
            buffer.beginWindow();
            buffer.add(row("long", 11, 10, 1, 0, "first"));
            buffer.add(row("long", 23, 10, 3, 0, "later"));
            buffer.add(row("long", 35, 10, 7, 0, null));
            assertThat(read(buffer.committedTransactions().get(0))).extracting(LogMinerRow::redo).containsExactly("first", "later");
            buffer.release(buffer.committedTransactions().get(0));
            assertThat(buffer.completeWindow(40)).isEqualTo(40);
            assertThat(buffer.memoryBytes()).isZero();
            assertThat(buffer.diskBytes()).isZero();
            assertThat(buffer.bufferedRecords()).isZero();
        }
        assertEmptyDirectory();
    }

    @ParameterizedTest
    @ValueSource(longs = { 512, 4096 })
    void shouldReleasePreviouslyDispatchedTransactionsOnlyAtCommit(long memory) throws Exception {
        try (var buffer = buffer(memory)) {
            buffer.beginWindow(new DispatchProgress(100, "checkpoint"));
            for (int operation : List.of(7, 38)) {
                buffer.add(row("old", 11, 10, 1, 0, "already dispatched"));
                buffer.add(row("old", 12, 10, 15, 0, null));
                buffer.add(row("old", 13, 10, 36, 0, null));
                assertThat(buffer.openTransactions()).isEqualTo(1);
                assertThat(buffer.bufferedRecords()).isEqualTo(1);
                assertThat(buffer.memoryBytes()).isPositive();
                if (memory == 512) {
                    assertThat(buffer.diskBytes()).isPositive();
                }
                else {
                    assertThat(buffer.diskBytes()).isZero();
                }

                buffer.add(row("old", 20, 10, operation, 0, null));

                assertThat(buffer.committedTransactions()).isEmpty();
                assertThat(buffer.openTransactions()).isZero();
                assertThat(buffer.bufferedRecords()).isZero();
                assertThat(buffer.memoryBytes()).isZero();
                assertThat(buffer.diskBytes()).isZero();
                try (var files = Files.walk(directory)) {
                    assertThat(files.filter(Files::isRegularFile).toList()).isEmpty();
                }
            }
            assertThat(buffer.completeWindow(100)).isEqualTo(100);
        }
        assertEmptyDirectory();
    }

    @Test
    void shouldRejectTheWindowIfDiscardingADispatchedTransactionCannotDeleteItsSpillFile() throws Exception {
        try (var buffer = buffer(512)) {
            buffer.beginWindow(new DispatchProgress(100, "checkpoint"));
            buffer.add(row("old", 11, 10, 1, 0, "already dispatched"));
            buffer.finishWindow();
            final Path file;
            try (var files = Files.walk(directory)) {
                file = files.filter(Files::isRegularFile).findFirst().orElseThrow();
            }
            Files.delete(file);
            Files.createDirectory(file);
            final var obstruction = Files.writeString(file.resolve("obstruction"), "prevent deletion");
            try {
                assertThatThrownBy(() -> buffer.add(row("old", 20, 10, 7, 0, null)))
                        .isInstanceOf(LogMinerSpillStore.StorageException.class);
                assertThatThrownBy(buffer::finishWindow).isInstanceOf(LogMinerSpillStore.StorageException.class);
                assertThatThrownBy(buffer::committedTransactions).isInstanceOf(LogMinerSpillStore.StorageException.class);
                assertThatThrownBy(() -> buffer.completeWindow(100)).isInstanceOf(LogMinerSpillStore.StorageException.class);
            }
            finally {
                Files.delete(obstruction);
                Files.delete(file);
            }
        }
        assertEmptyDirectory();
    }

    @Test
    void shouldRetainTheCheckpointTransactionAndLaterTransactionsForDispatch() {
        try (var buffer = buffer(4096)) {
            buffer.beginWindow(new DispatchProgress(100, "b"));
            buffer.add(row("z", 90, 90, 1, 0, "earlier commit"));
            buffer.add(row("a", 91, 91, 1, 0, "earlier transaction"));
            buffer.add(row("b", 92, 92, 1, 0, "checkpoint transaction"));
            buffer.add(row("c", 93, 93, 1, 0, "later transaction"));
            buffer.add(row("z", 99, 90, 7, 0, null));
            buffer.add(row("a", 100, 91, 7, 0, null));
            buffer.add(row("b", 100, 92, 7, 0, null));
            buffer.add(row("c", 100, 93, 7, 0, null));

            final var committed = buffer.committedTransactions();
            assertThat(committed).extracting(LogMinerTransactionBuffer.Transaction::id).containsExactly("b", "c");
            // Even a fully dispatched checkpoint transaction must reach the dispatcher
            // to finish restored transaction metadata and invoke commit callbacks.
            assertThat(read(committed.get(0))).extracting(LogMinerRow::redo).containsExactly("checkpoint transaction");
            assertThat(read(committed.get(1))).extracting(LogMinerRow::redo).containsExactly("later transaction");
        }
    }

    @Test
    void shouldDiscardTransactionsThroughASnapshotCommitBoundaryWithoutATransactionId() {
        try (var buffer = buffer(4096)) {
            buffer.beginWindow(new DispatchProgress(100, null));
            buffer.add(row("before", 90, 90, 1, 0, "before snapshot"));
            buffer.add(row("boundary", 91, 91, 1, 0, "at snapshot"));
            buffer.add(row("after", 92, 92, 1, 0, "after snapshot"));
            buffer.add(row("before", 99, 90, 7, 0, null));
            buffer.add(row("boundary", 100, 91, 38, 0, null));
            buffer.add(row("after", 101, 92, 7, 0, null));

            final var committed = buffer.committedTransactions();
            assertThat(committed).extracting(LogMinerTransactionBuffer.Transaction::id).containsExactly("after");
            assertThat(read(committed.get(0))).extracting(LogMinerRow::redo).containsExactly("after snapshot");
        }
    }

    @Test
    void shouldPreserveOlderOpenTransactionsAndTransactionsThatCommitAfterTheCheckpoint() {
        try (var buffer = buffer(4096)) {
            buffer.beginWindow(new DispatchProgress(100, "checkpoint"));
            buffer.add(row("later", 11, 10, 1, 0, "old row with a later commit"));
            buffer.add(row("open", 21, 20, 1, 0, "still pending"));
            assertThat(buffer.openTransactions()).isEqualTo(2);
            buffer.add(row("later", 110, 10, 7, 0, null));

            final var committed = buffer.committedTransactions();
            assertThat(committed).extracting(LogMinerTransactionBuffer.Transaction::id).containsExactly("later");
            assertThat(read(committed.get(0))).extracting(LogMinerRow::redo).containsExactly("old row with a later commit");
            buffer.release(committed.get(0));
            assertThat(buffer.openTransactions()).isEqualTo(1);
            assertThat(buffer.bufferedRecords()).isEqualTo(1);
            assertThat(buffer.completeWindow(120)).isEqualTo(20);
        }
    }

    @ParameterizedTest
    @ValueSource(longs = { 1024, 4096 })
    void shouldReconstructPreviouslyDispatchedHistoryWithoutExhaustingTheTransactionLimit(long memory) throws Exception {
        try (var buffer = new LogMinerTransactionBuffer(memory, 8192, 3, directory, 1048576)) {
            buffer.beginWindow();
            buffer.add(row("long", 11, 10, 1, 0, "pending long transaction"));
            buffer.add(row("a", 12, 12, 1, 0, "first short transaction"));
            buffer.add(row("a", 13, 12, 7, 0, null));
            buffer.release(buffer.committedTransactions().get(0));
            assertThat(buffer.completeWindow(20)).isEqualTo(10);

            buffer.beginWindow(new DispatchProgress(13, "a"));
            buffer.add(row("b", 22, 21, 1, 0, "second short transaction"));
            buffer.add(row("b", 24, 21, 7, 0, null));
            buffer.release(buffer.committedTransactions().get(0));
            assertThat(buffer.completeWindow(30)).isEqualTo(10);

            buffer.beginWindow(new DispatchProgress(24, "b"));
            buffer.add(row("c", 32, 31, 1, 0, "checkpoint transaction"));
            buffer.add(row("c", 34, 31, 7, 0, null));
            buffer.release(buffer.committedTransactions().get(0));
            assertThat(buffer.completeWindow(40)).isEqualTo(10);

            buffer.beginWindow(new DispatchProgress(34, "c"));
            buffer.add(row("long", 45, 0, 7, 0, null));
            assertThat(buffer.requiresReconstruction()).isTrue();
            buffer.resetForReconstruction();
            buffer.beginWindow(new DispatchProgress(34, "c"));
            // A real full-range replay also includes the short transactions that
            // were dispatched and released in the earlier forward windows.
            buffer.add(row("long", 11, 10, 1, 0, "pending long transaction"));
            buffer.add(row("a", 12, 12, 1, 0, "first short transaction"));
            buffer.add(row("a", 13, 12, 7, 0, null));
            buffer.add(row("b", 22, 21, 1, 0, "second short transaction"));
            buffer.add(row("b", 24, 21, 7, 0, null));
            buffer.add(row("c", 32, 31, 1, 0, "checkpoint transaction"));
            buffer.add(row("c", 34, 31, 7, 0, null));
            buffer.add(row("long", 45, 10, 7, 0, null));

            final var committed = buffer.committedTransactions();
            assertThat(committed).extracting(LogMinerTransactionBuffer.Transaction::id).containsExactly("c", "long");
            assertThat(read(committed.get(0))).extracting(LogMinerRow::redo).containsExactly("checkpoint transaction");
            assertThat(read(committed.get(1))).extracting(LogMinerRow::redo).containsExactly("pending long transaction");
            committed.forEach(buffer::release);
            assertThat(buffer.completeWindow(50)).isEqualTo(50);
            assertThat(buffer.memoryBytes()).isZero();
            assertThat(buffer.diskBytes()).isZero();
            assertThat(buffer.bufferedRecords()).isZero();
            assertThat(buffer.isCapacityExceeded()).isFalse();
        }
        assertEmptyDirectory();
    }

    @Test
    void shouldUseTheDispatchProgressProvidedForEachWindow() {
        try (var buffer = buffer(4096)) {
            buffer.beginWindow(new DispatchProgress(100, "z"));
            buffer.add(row("a", 11, 10, 1, 0, "discard"));
            buffer.add(row("a", 20, 10, 7, 0, null));
            assertThat(buffer.committedTransactions()).isEmpty();
            buffer.completeWindow(20);

            buffer.beginWindow();
            buffer.add(row("b", 21, 21, 1, 0, "no checkpoint"));
            buffer.add(row("b", 30, 21, 7, 0, null));
            assertThat(buffer.committedTransactions()).extracting(LogMinerTransactionBuffer.Transaction::id).containsExactly("b");
            buffer.release(buffer.committedTransactions().get(0));
            buffer.completeWindow(30);

            buffer.beginWindow(new DispatchProgress(30, "b"));
            buffer.add(row("c", 31, 31, 1, 0, "new checkpoint"));
            buffer.add(row("c", 40, 31, 7, 0, null));
            assertThat(buffer.committedTransactions()).extracting(LogMinerTransactionBuffer.Transaction::id).containsExactly("c");
        }
    }

    @Test
    void shouldSpillAgainstTheAggregateMemoryThresholdAndRetainUnicodeAndLongSql() throws Exception {
        final String sql = "中文\uD800\n'\\".repeat(20000);
        try (var buffer = buffer(2048)) {
            buffer.add(row("a", 11, 10, 1, 0, "small"));
            buffer.add(row("b", 13, 12, 1, 0, "small"));
            buffer.add(row("a", 14, 10, 3, 0, sql));
            buffer.add(row("b", 16, 12, 7, 0, null));
            buffer.add(row("a", 17, 10, 7, 0, null));
            buffer.finishWindow();
            assertThat(buffer.memoryBytes()).isLessThanOrEqualTo(2048);
            assertThat(buffer.spilledTransactions()).isPositive();
            assertThat(buffer.committedTransactions()).extracting(LogMinerTransactionBuffer.Transaction::id).containsExactly("b", "a");
            assertThat(read(buffer.committedTransactions().get(1))).containsExactly(row("a", 11, 10, 1, 0, "small"), row("a", 14, 10, 3, 0, sql));
        }
        assertEmptyDirectory();
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void shouldDiscardFullRollbackWithOrWithoutAnEarlierWindow(boolean earlierWindow) throws Exception {
        try (var buffer = buffer(512)) {
            buffer.add(row("a", 10, 10, 6, earlierWindow ? 0 : 1, null));
            buffer.add(row("a", 11, 10, 1, earlierWindow ? 0 : 1, "discard"));
            if (earlierWindow) {
                assertThat(buffer.completeWindow(20)).isEqualTo(10);
                buffer.beginWindow();
            }
            buffer.add(row("a", 25, earlierWindow ? 0 : 10, 36, 1, null));
            buffer.finishWindow();
            if (earlierWindow) {
                assertThat(buffer.requiresReconstruction()).isTrue();
                buffer.resetForReconstruction();
                buffer.beginWindow();
                buffer.add(row("a", 10, 10, 6, 1, null));
                buffer.add(row("a", 11, 10, 1, 1, "discard"));
                buffer.add(row("a", 25, 10, 36, 1, null));
            }
            assertThat(buffer.committedTransactions()).isEmpty();
            assertThat(buffer.completeWindow(30)).isEqualTo(30);
            assertThat(buffer.diskBytes()).isZero();
        }
        assertEmptyDirectory();
    }

    @ParameterizedTest
    @EnumSource(LogMiningBufferType.class)
    void shouldReleaseFullRollbackSuffixWithoutStart(LogMiningBufferType type) throws Exception {
        try (var buffer = new LogMinerTransactionBuffer(4096, 8192, 10, directory, 1048576, 0, type)) {
            buffer.add(row("a", 180, 0, 1, 1, "discard"));
            buffer.add(row("a", 200, 0, 36, 1, null));

            assertThat(buffer.committedTransactions()).isEmpty();
            assertThat(buffer.completeWindow(210)).isEqualTo(210);
            assertThat(buffer.openTransactions()).isZero();
            assertThat(buffer.bufferedRecords()).isZero();
            assertThat(buffer.memoryBytes()).isZero();
            assertThat(buffer.diskBytes()).isZero();
        }
        assertEmptyDirectory();
    }

    @ParameterizedTest
    @EnumSource(LogMiningBufferType.class)
    void shouldReleaseFullRollbackWhenOnlyTheTerminalRecordIsReplayed(LogMiningBufferType type) throws Exception {
        try (var buffer = new LogMinerTransactionBuffer(4096, 8192, 10, directory, 1048576, 0, type)) {
            buffer.add(row("a", 200, 0, 36, 1, null));

            assertThat(buffer.completeWindow(210)).isEqualTo(210);
            assertThat(buffer.openTransactions()).isZero();
            assertThat(buffer.memoryBytes()).isZero();
            assertThat(buffer.committedTransactions()).isEmpty();
        }
        assertEmptyDirectory();
    }

    @ParameterizedTest
    @EnumSource(LogMiningBufferType.class)
    void shouldDiscardNonemptyCacheOnFullRollback(LogMiningBufferType type) throws Exception {
        // Exercise both START flag values independently of the cached row count.
        for (int startRollback : List.of(0, 1)) {
            try (var buffer = new LogMinerTransactionBuffer(type == LogMiningBufferType.MEMORY ? 4096 : 512,
                    8192, 10, directory, 1048576, 0, type)) {
                buffer.add(row("a", 10, 10, 6, startRollback, null));
                buffer.add(row("a", 11, 10, 1, 0, "provisional data"));
                assertThat(buffer.bufferedRecords()).isEqualTo(1);
                assertThat(buffer.spilledTransactions()).isEqualTo(type == LogMiningBufferType.MEMORY ? 0 : 1);
                buffer.add(row("a", 20, 10, 36, 1, null));
                buffer.add(row("a", 20, 10, 36, 1, null));

                assertThat(buffer.committedTransactions()).isEmpty();
                assertThat(buffer.completeWindow(30)).isEqualTo(30);
                assertThat(buffer.openTransactions()).isZero();
                assertThat(buffer.bufferedRecords()).isZero();
                assertThat(buffer.memoryBytes()).isZero();
                assertThat(buffer.diskBytes()).isZero();
                try (var files = Files.walk(directory)) {
                    assertThat(files.filter(Files::isRegularFile).toList()).isEmpty();
                }
            }
            assertEmptyDirectory();
        }
    }

    @ParameterizedTest
    @EnumSource(LogMiningBufferType.class)
    void shouldNotPinReplayWhenReconstructionReintroducesARolledBackSuffix(LogMiningBufferType type) throws Exception {
        try (var buffer = new LogMinerTransactionBuffer(4096, 8192, 10, directory, 1048576, 0, type)) {
            buffer.add(row("a", 100, 100, 6, 1, null));
            buffer.add(row("b", 150, 150, 6, 0, null));
            buffer.add(row("a", 180, 100, 1, 1, "discard"));
            buffer.add(row("a", 200, 100, 36, 1, null));
            assertThat(buffer.completeWindow(210)).isEqualTo(150);
            assertThat(buffer.openTransactions()).isEqualTo(1);

            buffer.beginWindow();
            buffer.add(row("b", 230, 0, 7, 0, null));
            assertThat(buffer.requiresReconstruction()).isTrue();
            assertThatThrownBy(() -> buffer.completeWindow(240)).hasMessageContaining("must be reconstructed");
            buffer.resetForReconstruction();
            buffer.beginWindow();
            // Rebuild [150, 240]: A's START is outside the retained range, even without a restart.
            buffer.add(row("b", 150, 150, 6, 0, null));
            buffer.add(row("a", 180, 0, 1, 1, "discard"));
            buffer.add(row("a", 200, 0, 36, 1, null));
            buffer.add(row("b", 230, 150, 7, 0, null));
            final var committed = buffer.committedTransactions();
            assertThat(committed).extracting(LogMinerTransactionBuffer.Transaction::id).containsExactly("b");
            buffer.release(committed.get(0));

            assertThat(buffer.completeWindow(240)).isEqualTo(240);
            assertThat(buffer.openTransactions()).isZero();
            assertThat(buffer.memoryBytes()).isZero();
            buffer.beginWindow();
            assertThat(buffer.completeWindow(300)).isEqualTo(300);
        }
        assertEmptyDirectory();
    }

    @ParameterizedTest
    @EnumSource(LogMiningBufferType.class)
    void shouldRequireAllThreeFullRollbackMarkerFields(LogMiningBufferType type) throws Exception {
        for (var marker : List.of(
                row("a", 12, 10, 36, 1, null, "9"),
                row("a", 12, 10, 36, 1, null, null),
                row("a", 12, 10, 36, 0, null, "-1"),
                row("a", 12, 10, 1, 1, "discard", "-1"))) {
            try (var buffer = new LogMinerTransactionBuffer(4096, 8192, 10, directory, 1048576, 0, type)) {
                buffer.add(row("a", 10, 10, 6, 1, null));
                buffer.add(row("a", 11, 10, 1, 1, "discard"));
                buffer.add(marker);

                assertThat(buffer.bufferedRecords()).isZero();
                assertThat(buffer.completeWindow(20)).isEqualTo(10);
                assertThat(buffer.openTransactions()).isEqualTo(1);
                assertThat(buffer.committedTransactions()).isEmpty();
            }
            assertEmptyDirectory();
        }
    }

    @ParameterizedTest
    @EnumSource(LogMiningBufferType.class)
    void shouldReplaceProvisionalImagesWithTheFinalRollbackView(LogMiningBufferType type) throws Exception {
        try (var buffer = new LogMinerTransactionBuffer(type == LogMiningBufferType.MEMORY ? 4096 : 512, 8192, 10, directory, 1048576, 0, type)) {
            buffer.add(row("a", 11, 10, 1, 0, "keep"));
            buffer.add(row("a", 12, 10, 36, 0, null));
            buffer.add(row("a", 13, 10, 1, 0, "discard"));
            buffer.completeWindow(20);
            buffer.beginWindow();
            buffer.add(row("a", 21, 0, 36, 0, null));
            buffer.add(row("a", 25, 0, 7, 0, null));
            assertThat(buffer.requiresReconstruction()).isTrue();
            assertThatThrownBy(buffer::committedTransactions).hasMessageContaining("must be reconstructed");
            assertThatThrownBy(() -> buffer.completeWindow(30)).hasMessageContaining("must be reconstructed");
            buffer.resetForReconstruction();
            assertThat(buffer.diskBytes()).isZero();
            assertThat(buffer.bufferedRecords()).isZero();
            buffer.beginWindow();
            buffer.add(row("a", 11, 10, 1, 0, "keep"));
            buffer.add(row("a", 12, 10, 36, 0, null));
            buffer.add(row("a", 13, 10, 1, 1, "discard"));
            buffer.add(row("a", 21, 10, 36, 0, null));
            buffer.add(row("a", 25, 10, 7, 0, null));
            assertThat(read(buffer.committedTransactions().get(0))).extracting(LogMinerRow::redo).containsExactly("keep");
            assertThat(buffer.spilledTransactions()).isEqualTo(type == LogMiningBufferType.MEMORY ? 0 : 2);
        }
        assertEmptyDirectory();
    }

    @ParameterizedTest
    @EnumSource(LogMiningBufferType.class)
    void shouldRetainAnEmptyTransactionAfterPartialRollbackUntilItActuallyEnds(LogMiningBufferType type) throws Exception {
        try (var buffer = new LogMinerTransactionBuffer(type == LogMiningBufferType.MEMORY ? 4096 : 512,
                8192, 10, directory, 1048576, 0, type)) {
            buffer.add(row("a", 10, 10, 6, 1, null));
            buffer.add(row("a", 11, 10, 1, 1, "rolled back"));
            buffer.add(row("a", 12, 10, 36, 1, null, "9"));
            buffer.add(row("a", 13, 10, 36, 0, null));
            assertThat(buffer.completeWindow(20)).isEqualTo(10);
            assertThat(buffer.openTransactions()).isEqualTo(1);
            buffer.beginWindow();
            buffer.add(row("a", 21, 0, 1, 0, "later"));
            assertThat(buffer.requiresReconstruction()).isFalse();
            assertThat(buffer.completeWindow(30)).isEqualTo(10);
            buffer.beginWindow();
            buffer.add(row("a", 35, 0, 7, 0, null));
            assertThat(buffer.requiresReconstruction()).isTrue();
            buffer.resetForReconstruction();
            buffer.beginWindow();
            buffer.add(row("a", 10, 10, 6, 1, null));
            buffer.add(row("a", 11, 10, 1, 1, "rolled back"));
            buffer.add(row("a", 12, 10, 36, 1, null, "9"));
            buffer.add(row("a", 13, 10, 36, 0, null));
            buffer.add(row("a", 21, 10, 1, 0, "later"));
            buffer.add(row("a", 35, 10, 7, 0, null));
            final var committed = buffer.committedTransactions();
            assertThat(committed).hasSize(1);
            assertThat(read(committed.get(0))).extracting(LogMinerRow::redo).containsExactly("later");
            buffer.release(committed.get(0));
            assertThat(buffer.completeWindow(40)).isEqualTo(40);
            assertThat(buffer.openTransactions()).isZero();
            assertThat(buffer.bufferedRecords()).isZero();
            assertThat(buffer.memoryBytes()).isZero();
            assertThat(buffer.diskBytes()).isZero();
        }
        assertEmptyDirectory();
    }

    @Test
    void shouldRejectDiskQuotaAndCleanOnlyItsOwnFiles() throws Exception {
        final var unrelated = Files.writeString(directory.resolve("unrelated"), "keep");
        try (var buffer = new LogMinerTransactionBuffer(512, 1048576, 10000, directory, 32)) {
            assertThatThrownBy(() -> buffer.add(row("a", 11, 10, 1, 0, "value")))
                    .isInstanceOf(LogMinerSpillStore.StorageException.class).hasMessageContaining("spill.max.bytes");
        }
        assertThat(Files.readString(unrelated)).isEqualTo("keep");
        try (var files = Files.list(directory)) {
            assertThat(files.toList()).containsExactly(unrelated);
        }
    }

    @Test
    void shouldFailWhenSpillPathIsNotADirectory() throws Exception {
        final var invalid = Files.writeString(directory.resolve("file"), "not a directory");
        try (var buffer = new LogMinerTransactionBuffer(512, 1048576, 10000, invalid, 1048576)) {
            assertThatThrownBy(() -> buffer.add(row("a", 11, 10, 1, 0, "value")))
                    .isInstanceOf(LogMinerSpillStore.StorageException.class).hasMessageContaining("spill.path");
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "truncate", "checksum", "length" })
    void shouldRejectDamagedTransactionFiles(String damage) throws Exception {
        try (var buffer = buffer(512)) {
            buffer.add(row("a", 11, 10, 1, 0, "value"));
            buffer.add(row("a", 15, 10, 7, 0, null));
            buffer.finishWindow();
            final Path file;
            try (var files = Files.walk(directory)) {
                file = files.filter(Files::isRegularFile).findFirst().orElseThrow();
            }
            final var bytes = Files.readAllBytes(file);
            if (damage.equals("truncate")) {
                Files.write(file, new byte[0], StandardOpenOption.TRUNCATE_EXISTING);
            }
            else {
                bytes[damage.equals("length") ? 0 : bytes.length - 1] ^= 127;
                Files.write(file, bytes, StandardOpenOption.TRUNCATE_EXISTING);
            }
            assertThatThrownBy(() -> read(buffer.committedTransactions().get(0)))
                    .isInstanceOf(LogMinerSpillStore.StorageException.class);
        }
        assertEmptyDirectory();
    }

    @Test
    void shouldBoundMetadataEvenAfterAllRowsHaveSpilled() {
        try (var buffer = buffer(512)) {
            buffer.add(row("a", 11, 10, 1, 0, "value"));
            assertThatThrownBy(() -> buffer.add(row("b", 13, 12, 1, 0, "value"))).hasMessageContaining("metadata exceeds");
        }
    }

    @Test
    void shouldReassembleContinuationsBeforeSpillingAndRejectMissingPieces() {
        try (var buffer = buffer(512)) {
            final var first = row("a", 11, 10, 1, 0, "part one");
            buffer.add(new LogMinerRow(first.lsn(), first.startLsn(), 0, first.transactionId(), 1, 0, null, null, null,
                    first.redo(), first.recordSetId(), 0, 1, first.timestamp(), 7));
            assertThatThrownBy(buffer::finishWindow).hasMessageContaining("Incomplete");
            buffer.add(piece(1, 0, "part two"));
            buffer.add(row("a", 15, 10, 7, 0, null));
            buffer.finishWindow();
            assertThat(read(buffer.committedTransactions().get(0))).extracting(LogMinerRow::redo).containsExactly("part onepart two");
        }
    }

    @Test
    void shouldKeepCumulativeBytesAndRowsAsTelemetryBeyondTheFormerDefaults() {
        try (var buffer = buffer(512)) {
            final var ignored = row(null, 10, 10, 37, 0, "x".repeat(512));
            for (int i = 0; i < 1_000_001; i++) {
                buffer.add(ignored);
            }
            buffer.add(row("a", 11, 10, 1, 0, "keep"));
            buffer.add(row("a", 12, 10, 7, 0, null));
            assertThat(buffer.windowBytes()).isGreaterThan(512L * 1024 * 1024);
            assertThat(buffer.windowRecords()).isGreaterThan(1_000_000);
            assertThat(buffer.isCapacityExceeded()).isFalse();
            assertThat(read(buffer.committedTransactions().get(0))).extracting(LogMinerRow::redo).containsExactly("keep");
            buffer.release(buffer.committedTransactions().get(0));
            assertThat(buffer.completeWindow(20)).isEqualTo(20);
            buffer.beginWindow();
            assertThat(buffer.windowBytes()).isZero();
            assertThat(buffer.windowRecords()).isZero();
        }
    }

    @Test
    void shouldSpillManyRowsWithoutConfusingSqlAndResidentTransactionLimitsWithCumulativeWork() throws Exception {
        try (var buffer = new LogMinerTransactionBuffer(512, 8192, 1, directory, 4 * 1024 * 1024)) {
            final var payload = "中".repeat(4096);
            for (int i = 0; i < 256; i++) {
                buffer.add(row("a", 11 + i, 10, 1, 0, payload));
            }
            buffer.add(row("a", 300, 10, 7, 0, null));
            assertThat(buffer.memoryBytes()).isLessThanOrEqualTo(512);
            assertThat(buffer.diskBytes()).isGreaterThan(2 * 1024 * 1024);
            assertThat(buffer.windowBytes()).isGreaterThan(2 * 1024 * 1024);
            final var tx = buffer.committedTransactions().get(0);
            try (var cursor = tx.open()) {
                for (int i = 0; i < 256; i++) {
                    final var record = cursor.next();
                    assertThat(record.lsn()).isEqualTo(11 + i);
                    assertThat(record.redo()).isEqualTo(payload);
                }
                assertThat(cursor.next()).isNull();
            }
            buffer.release(tx);
            assertThat(buffer.completeWindow(300)).isEqualTo(300);
            assertThat(buffer.diskBytes()).isZero();
            // A released transaction no longer consumes the metadata count limit.
            buffer.beginWindow();
            buffer.add(row("b", 301, 301, 1, 0, "next"));
            assertThat(buffer.openTransactions()).isEqualTo(1);
        }
        assertEmptyDirectory();
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void shouldBoundOneSqlBeforeConcatenationAndPreventReuseAfterCapacityFailure(boolean continuation) {
        try (var buffer = new LogMinerTransactionBuffer(512, 8, 2, directory, 1048576)) {
            final var first = row("a", 11, 10, 1, 0, "中😀");
            if (continuation) {
                buffer.add(new LogMinerRow(first.lsn(), first.startLsn(), 0, first.transactionId(), 1, 0, null, null, null,
                        first.redo(), first.recordSetId(), 0, 1, first.timestamp(), 7));
            }
            assertThatThrownBy(() -> buffer.add(piece(continuation ? 1 : 0, 0, continuation ? "ab" : "中😀ab")))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("log.mining.buffer.max.sql.bytes=8");
            assertThat(buffer.isCapacityExceeded()).isTrue();
            assertThatThrownBy(buffer::committedTransactions).hasMessageContaining("max.sql.bytes");
            assertThatThrownBy(() -> buffer.completeWindow(20)).hasMessageContaining("max.sql.bytes");
            assertThatThrownBy(buffer::resetForReconstruction).hasMessageContaining("max.sql.bytes");
            assertThatThrownBy(() -> buffer.add(row("a", 12, 10, 7, 0, null))).hasMessageContaining("max.sql.bytes");
        }
    }

    @Test
    void shouldAllowExactlyTheSqlLimitAndPreserveFirstPieceMetadataWhenSpilling() {
        try (var buffer = new LogMinerTransactionBuffer(512, 8, 2, directory, 1048576)) {
            final var first = new LogMinerRow(11, 10, 0, "a", 1, 0, "APP", "T", "writer", "中", "r", 0, 1, Instant.EPOCH,
                    7, 0, 0, 123, 45);
            buffer.add(first);
            buffer.add(new LogMinerRow(11, 0, 0, "a", 1, 0, null, null, null, "😀a", "r", 1, 0, Instant.EPOCH, 0));
            buffer.add(row("a", 12, 10, 7, 0, null));
            assertThat(buffer.diskBytes()).isPositive();
            assertThat(read(buffer.committedTransactions().get(0))).containsExactly(first.withRedo("中😀a"));
        }
    }

    @Test
    void shouldAssembleThreePiecesBeforePublishingTheTransaction() {
        try (var buffer = buffer(512)) {
            buffer.add(piece(0, 1, "first"));
            buffer.add(piece(1, 1, "middle"));
            buffer.add(piece(2, 0, "last"));
            buffer.add(row("a", 12, 10, 7, 0, null));
            assertThat(read(buffer.committedTransactions().get(0))).extracting(LogMinerRow::redo).containsExactly("firstmiddlelast");
        }
    }

    @ParameterizedTest
    @ValueSource(ints = { -1, 0, 2, Integer.MAX_VALUE })
    void shouldRejectMissingRepeatedAndOutOfOrderContinuationSequences(int sequence) {
        try (var buffer = buffer(512)) {
            buffer.add(piece(0, 1, "first"));
            assertThatThrownBy(() -> buffer.add(piece(sequence, 0, "last"))).hasMessageContaining("SSN");
            assertThatThrownBy(buffer::committedTransactions).hasMessageContaining("SSN");
            assertThatThrownBy(() -> buffer.completeWindow(20)).hasMessageContaining("SSN");
            assertThatThrownBy(buffer::resetForReconstruction).hasMessageContaining("SSN");
        }
    }

    @ParameterizedTest
    @ValueSource(ints = { -1, 1, 2 })
    void shouldRejectAnOrphanSqlSuffix(int sequence) {
        try (var buffer = buffer(512)) {
            assertThatThrownBy(() -> buffer.add(piece(sequence, 0, "suffix"))).hasMessageContaining("SSN");
        }
    }

    @ParameterizedTest
    @org.junit.jupiter.params.provider.NullAndEmptySource
    void shouldRejectMissingSqlInAnyContinuationPiece(String sql) {
        try (var buffer = buffer(512)) {
            assertThatThrownBy(() -> buffer.add(piece(0, 1, sql))).hasMessageContaining("SQL_REDO");
        }
        try (var buffer = buffer(512)) {
            buffer.add(piece(0, 1, "first"));
            assertThatThrownBy(() -> buffer.add(piece(1, 0, sql))).hasMessageContaining("SQL_REDO");
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "CSF", "ROLL_BACK", "RS_ID", "XID", "LSN", "OPERATION", "OBJECT", "VERSION", "ROLLBACK_MISMATCH" })
    void shouldRejectInvalidFlagsAndConflictingContinuationIdentity(String damage) {
        try (var buffer = buffer(512)) {
            final var first = new LogMinerRow(11, 10, 0, "a", 1, 0, null, null, null, "first", "r", 0, 1, Instant.EPOCH,
                    7, 0, 0, 123, 45);
            buffer.add(first);
            final var next = new LogMinerRow(damage.equals("LSN") ? 12 : 11, 10, 0, damage.equals("XID") ? "b" : "a",
                    damage.equals("OPERATION") ? 3 : 1, damage.equals("ROLL_BACK") ? 2 : damage.equals("ROLLBACK_MISMATCH") ? 1 : 0,
                    null, null, null, "last", damage.equals("RS_ID") ? null : "r", 1, damage.equals("CSF") ? 2 : 0, Instant.EPOCH,
                    damage.equals("OBJECT") ? 8 : 7, damage.equals("VERSION") ? 2 : 0, 0, 123, 45);
            assertThatThrownBy(() -> buffer.add(next)).isInstanceOf(IllegalStateException.class).hasMessageContaining("LSN");
            assertThatThrownBy(buffer::committedTransactions).isInstanceOf(IllegalStateException.class);
        }
    }

    @Test
    void shouldRejectContinuationPiecesWithDifferentUnknownOperationCodes() {
        try (var buffer = buffer(512)) {
            final var first = new LogMinerRow(11, 10, 0, "a", 999, 0, null, null, null, "first", "r", 0, 1, Instant.EPOCH, 7);
            final var next = new LogMinerRow(11, 10, 0, "a", 1000, 0, null, null, null, "last", "r", 1, 0, Instant.EPOCH, 7);
            assertThat(first.eventType()).isEqualTo(next.eventType()).isEqualTo(EventType.UNSUPPORTED);
            buffer.add(first);
            assertThatThrownBy(() -> buffer.add(next)).isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("Interleaved or missing Dameng SQL continuation");
        }
    }

    private static LogMinerRow piece(int sequence, int continuation, String sql) {
        return new LogMinerRow(11, 10, 0, "a", 1, 0, null, null, null, sql, "r", sequence, continuation, Instant.EPOCH, 7);
    }

    @Test
    void shouldBoundResidentTransactionEntriesEvenWhenTheirRowsAreDiscarded() {
        try (var buffer = new LogMinerTransactionBuffer(1048576, 8192, 1, directory, 1048576)) {
            buffer.add(row("a", 11, 10, 1, 1, "discarded"));
            assertThatThrownBy(() -> buffer.add(row("b", 13, 12, 1, 1, "discarded")))
                    .hasMessageContaining("max.transactions=1");
            assertThat(buffer.isCapacityExceeded()).isTrue();
            assertThatThrownBy(() -> buffer.completeWindow(20)).hasMessageContaining("max.transactions=1");
        }
    }

    @ParameterizedTest
    @ValueSource(ints = { 0, 1, 2, 3 })
    void shouldRejectNonpositiveResourceLimits(int index) {
        final long[] limits = { 512, 8192, 10, 1048576 };
        limits[index] = 0;
        assertThatThrownBy(() -> new LogMinerTransactionBuffer(limits[0], limits[1], limits[2], directory, limits[3]))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldAppendSpilledRowsWithoutEvictingResidentTransactions() throws Exception {
        try (var buffer = new LogMinerTransactionBuffer(4096, 8192, 10, directory, 1048576)) {
            final var first = row("a", 11, 10, 1, 0, "a".repeat(2048));
            final var resident = row("b", 12, 10, 1, 0, "b".repeat(512));
            final var second = row("a", 13, 10, 1, 0, "c".repeat(2048));
            buffer.add(first);
            buffer.add(resident);
            assertThat(buffer.spilledTransactions()).isEqualTo(1);
            final long memory = buffer.memoryBytes();
            buffer.add(second);
            assertThat(buffer.spilledTransactions()).isEqualTo(1);
            assertThat(buffer.memoryBytes()).isEqualTo(memory);
            buffer.add(row("a", 14, 10, 7, 0, null));
            buffer.add(row("b", 15, 10, 7, 0, null));
            final var committed = buffer.committedTransactions();
            assertThat(read(committed.get(0))).containsExactly(first, second);
            assertThat(read(committed.get(1))).containsExactly(resident);
            committed.forEach(buffer::release);
            assertThat(buffer.memoryBytes()).isZero();
            assertThat(buffer.diskBytes()).isZero();
        }
        assertEmptyDirectory();
    }

    @ParameterizedTest
    @EnumSource(LogMiningBufferType.class)
    void shouldCountOnlyOpenTransactionsThroughStateChangesAndReplayDiscard(LogMiningBufferType type) {
        try (var buffer = new LogMinerTransactionBuffer(16384, 8192, 10, directory, 1048576, 0, type)) {
            buffer.beginWindow(new DispatchProgress(100, null));
            buffer.add(row("a", 10, 10, 6, 0, null));
            buffer.add(row("b", 11, 11, 6, 1, null));
            assertThat(buffer.openTransactions()).isEqualTo(2);
            buffer.add(row("b", 12, 11, 36, 0, null)); // Savepoint rollback is not terminal.
            assertThat(buffer.openTransactions()).isEqualTo(2);
            buffer.add(row("b", 13, 11, 36, 1, null));
            assertThat(buffer.openTransactions()).isEqualTo(1);
            buffer.add(row("b", 14, 11, 36, 1, null)); // Repeated marker must not decrement twice.
            assertThat(buffer.openTransactions()).isEqualTo(1);
            buffer.add(row("b", 15, 11, 36, 0, null)); // Unknown/nonterminal marker restores pending state.
            assertThat(buffer.openTransactions()).isEqualTo(2);
            buffer.add(row("b", 16, 11, 1, 0, "provisional"));
            assertThat(buffer.openTransactions()).isEqualTo(2);
            buffer.add(row("b", 17, 11, 36, 1, null)); // Full rollback completes even a nonempty cache.
            assertThat(buffer.openTransactions()).isEqualTo(1);
            buffer.add(row("a", 20, 10, 7, 0, null)); // Previously dispatched: release at COMMIT.
            assertThat(buffer.openTransactions()).isZero();
            buffer.add(row("b", 101, 11, 7, 0, null));
            buffer.add(row("b", 101, 11, 7, 0, null));
            assertThat(buffer.openTransactions()).isZero();
            buffer.release(buffer.committedTransactions().get(0));
            assertThat(buffer.openTransactions()).isZero();
            assertThat(buffer.memoryBytes()).isZero();
            buffer.add(row("c", 102, 102, 6, 0, null));
            buffer.add(row("d", 103, 103, 6, 1, null));
            buffer.add(row("d", 104, 103, 36, 1, null));
            assertThat(buffer.completeWindow(110)).isEqualTo(102);
            assertThat(buffer.openTransactions()).isEqualTo(1);
            buffer.resetForReconstruction();
            assertThat(buffer.openTransactions()).isZero();
            buffer.add(row("c", 102, 102, 1, 0, "rebuilt"));
            assertThat(buffer.openTransactions()).isEqualTo(1);
            buffer.close();
            assertThat(buffer.openTransactions()).isZero();
        }
    }

    @Test
    void shouldEvictGrowingLargeTransactionsAndForgetReleasedCandidates() throws Exception {
        try (var buffer = new LogMinerTransactionBuffer(10000, 8192, 10, directory, 1048576)) {
            for (String id : List.of("a", "b", "c")) {
                buffer.add(row(id, 11, 10, 1, 0, "x"));
            }
            buffer.add(row("b", 12, 10, 1, 0, "large".repeat(400)));
            buffer.add(row("d", 13, 10, 1, 0, "large".repeat(400)));
            assertThat(buffer.spilledTransactions()).isEqualTo(1);
            for (String id : List.of("a", "c", "b", "d")) {
                buffer.add(row(id, 20, 10, 7, 0, null));
                final var tx = buffer.committedTransactions().get(0);
                assertThat(read(tx)).hasSize(id.equals("b") ? 2 : 1);
                buffer.release(tx);
            }
            assertThat(buffer.memoryBytes()).isZero();
            assertThat(buffer.diskBytes()).isZero();
            for (int i = 0; i < 20; i++) {
                final var value = row("a", 30 + i, 30, 1, 0, "z".repeat(1000));
                buffer.add(value);
                buffer.add(row("a", 60 + i, 30, 7, 0, null));
                final var tx = buffer.committedTransactions().get(0);
                assertThat(read(tx)).containsExactly(value);
                buffer.release(tx);
            }
            // A stale eviction candidate would create extra files or corrupt the memory accounting.
            buffer.add(row("e", 100, 100, 1, 0, "large".repeat(800)));
            buffer.add(row("f", 101, 100, 1, 0, "large".repeat(800)));
            assertThat(buffer.spilledTransactions()).isEqualTo(2);
            assertThat(buffer.memoryBytes()).isBetween(1L, 10000L);
        }
        assertEmptyDirectory();
    }

    private LogMinerTransactionBuffer buffer(long memory) {
        return new LogMinerTransactionBuffer(memory, 4 * 1024 * 1024, 10000, directory, 8 * 1024 * 1024);
    }

    private List<LogMinerRow> read(LogMinerTransactionBuffer.Transaction transaction) {
        final List<LogMinerRow> rows = new ArrayList<>();
        try (var cursor = transaction.open()) {
            for (var row = cursor.next(); row != null; row = cursor.next()) {
                rows.add(row);
            }
        }
        return rows;
    }

    private void assertEmptyDirectory() throws Exception {
        try (var files = Files.list(directory)) {
            assertThat(files.toList()).isEmpty();
        }
    }

    private static LogMinerRow row(String xid, long lsn, long start, int operation, int rollback, String sql) {
        return row(xid, lsn, start, operation, rollback, sql, operation == 36 && rollback == 1 ? "-1" : "r");
    }

    private static LogMinerRow row(String xid, long lsn, long start, int operation, int rollback, String sql, String recordSetId) {
        return new LogMinerRow(lsn, start, 0, xid, operation, rollback, null, null, null, sql, recordSetId, 0, 0,
                Instant.parse("2026-10-16T01:02:03.123456789Z"), 7);
    }
}
