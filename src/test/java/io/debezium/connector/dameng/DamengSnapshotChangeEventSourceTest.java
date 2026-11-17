/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Instant;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.connector.base.ChangeEventQueueMetrics;
import io.debezium.jdbc.MainConnectionProvidingConnectionFactory;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.source.SnapshottingTask;
import io.debezium.pipeline.source.spi.ChangeEventSource.ChangeEventSourceContext;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.relational.Column;
import io.debezium.relational.RelationalSnapshotChangeEventSource.RelationalSnapshotContext;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables.TableFilter;
import io.debezium.snapshot.SnapshotterService;
import io.debezium.snapshot.spi.SnapshotQuery;
import io.debezium.util.Clock;

import ch.qos.logback.classic.Logger;

class DamengSnapshotChangeEventSourceTest {
    private static final TableId EXISTING = new TableId(null, "APP", "EXISTING");
    private static final TableId ADDED = new TableId(null, "APP", "ADDED");
    private DamengConnection connection;
    private ChangeEventSourceContext running;
    private SnapshotProgressListener<DamengPartition> listener;
    private SnapshotterService snapshotterService;
    private Clock clock;
    private final LogInterceptor logs = LogInterceptor.forClass(DamengSnapshotChangeEventSource.class);

    @AfterEach
    void stopCapturingLogs() {
        logs.stop();
        ((Logger) LoggerFactory.getLogger(DamengSnapshotChangeEventSource.class)).detachAppender(logs);
    }

    @BeforeEach
    @SuppressWarnings("unchecked")
    void before() throws Exception {
        connection = mock(DamengConnection.class);
        running = mock(ChangeEventSourceContext.class);
        listener = mock(SnapshotProgressListener.class);
        snapshotterService = mock(SnapshotterService.class);
        when(snapshotterService.getSnapshotLock()).thenReturn(new io.debezium.connector.dameng.snapshot.SharedSnapshotLock());
        clock = Clock.SYSTEM;
        when(connection.quotedTableIdString(any())).thenAnswer(invocation -> ((TableId) invocation.getArgument(0)).toDoubleQuotedString());
        when(connection.currentLsn()).thenReturn(100L, 200L, 300L);
        when(connection.databaseMagic()).thenReturn(42L);
        when(running.isRunning()).thenReturn(true);
    }

    @ParameterizedTest
    @EnumSource(value = DamengTableType.class, names = { "EXTERNAL", "TEMPORARY" })
    void shouldKeepNonRowIdentityInHistoryWithoutLockingOrScanningRows(DamengTableType kind) throws Exception {
        final var source = source(sharedConfiguration().build());
        final var context = context();
        context.capturedTables.add(ADDED);
        final var identity = new DamengTableType.Identity(kind, 71);
        when(connection.readNonRowTableIds()).thenReturn(Map.of(ADDED, identity));
        when(connection.getAllTableIds(null)).thenReturn(Set.of(EXISTING, ADDED));

        source.getAllTableIds(context);
        source.lockTablesForSchemaSnapshot(running, context);
        source.determineSnapshotOffset(context, null);
        source.readTableStructure(running, context, null, new SnapshottingTask(true, true, List.of(), Map.of(), false));

        verify(connection, never()).executeWithoutCommitting("LOCK TABLE \"APP\".\"ADDED\" IN SHARE MODE");
        verify(connection, never()).getTableObjectId(ADDED);
        final ArgumentCaptor<TableFilter> filter = ArgumentCaptor.forClass(TableFilter.class);
        verify(connection).readSchema(any(), isNull(), eq("APP"), filter.capture(), isNull(), eq(false));
        assertThat(filter.getValue().isIncluded(ADDED)).isFalse();
        assertThat(filter.getValue().isIncluded(EXISTING)).isTrue();
        assertThat(context.capturedTables).containsExactly(EXISTING);
        assertThat(context.capturedSchemaTables).containsExactlyInAnyOrder(EXISTING, ADDED);
        assertThat(context.tables.forTable(ADDED)).isEqualTo(identity.table(ADDED));
        assertThat(context.tables.forTable(ADDED).attributeWithName(DamengTableType.ATTRIBUTE_NAME).value()).isEqualTo(kind.name());
        assertThat(source.getTablesForSchemaChange(context)).containsExactly(EXISTING);
        verify(listener).dataCollectionSnapshotCompleted(context.partition, ADDED, 0);
    }

    @ParameterizedTest
    @CsvSource({ "EXTERNAL,0", "EXTERNAL,72", "TEMPORARY,0", "TEMPORARY,72" })
    void shouldRejectChangedNonRowIdentityAcrossSnapshotBoundary(DamengTableType kind, long replacement) throws Exception {
        final var source = source(sharedConfiguration().build());
        final var context = context();
        when(connection.readNonRowTableIds()).thenReturn(Map.of(EXISTING, new DamengTableType.Identity(kind, 71)),
                replacement == 0 ? Map.of() : Map.of(EXISTING, new DamengTableType.Identity(kind, replacement)));
        source.lockTablesForSchemaSnapshot(running, context);
        source.determineSnapshotOffset(context, null);
        assertThatThrownBy(() -> source.readTableStructure(running, context, null, new SnapshottingTask(true, true, List.of(), Map.of(), false)))
                .isInstanceOf(SQLException.class).hasMessageContaining("Non-row table identity changed");
        assertThat(context.tables.tableIds()).isEmpty();
        verify(connection, never()).readSchema(any(), any(), any(), any(), any(), eq(false));
    }

    @Test
    void shouldRejectANonRowKindChangeEvenWhenTheObjectNumberIsUnchanged() throws Exception {
        final var source = source(sharedConfiguration().build());
        final var context = context();
        when(connection.readNonRowTableIds()).thenReturn(Map.of(EXISTING, new DamengTableType.Identity(DamengTableType.EXTERNAL, 71)),
                Map.of(EXISTING, new DamengTableType.Identity(DamengTableType.TEMPORARY, 71)));
        source.lockTablesForSchemaSnapshot(running, context);
        source.determineSnapshotOffset(context, null);
        assertThatThrownBy(() -> source.readTableStructure(running, context, null, new SnapshottingTask(true, true, List.of(), Map.of(), false)))
                .isInstanceOf(SQLException.class).hasMessageContaining("Non-row table identity changed");
        verify(connection, never()).readSchema(any(), any(), any(), any(), any(), eq(false));
    }

    @ParameterizedTest
    @EnumSource(value = DamengTableType.class, names = { "EXTERNAL", "TEMPORARY" })
    @SuppressWarnings("unchecked")
    void shouldPersistNonRowOnlySnapshotWithoutPublishingSchemaEvents(DamengTableType kind) throws Exception {
        final var config = new DamengConnectorConfig(sharedConfiguration().build());
        final var schema = mock(DamengDatabaseSchema.class);
        final var dispatcher = mock(EventDispatcher.class);
        final MainConnectionProvidingConnectionFactory<DamengConnection> factory = mock(MainConnectionProvidingConnectionFactory.class);
        when(factory.mainConnection()).thenReturn(connection);
        final var source = new DamengSnapshotChangeEventSource(config, factory, schema, dispatcher, clock, listener,
                new NotificationService<>(List.of(), config, null, ignored -> {
                }), snapshotterService);
        final var context = context();
        context.offset = new DamengOffsetContext(config, 100);
        context.capturedTables.clear();
        final var external = new DamengTableType.Identity(kind, 71).table(EXISTING);
        context.tables.overwriteTable(external);

        source.createSchemaChangeEventsForTables(running, context, new SnapshottingTask(true, false, List.of(), Map.of(), false));

        final var event = ArgumentCaptor.forClass(io.debezium.schema.SchemaChangeEvent.class);
        verify(schema).applySchemaChange(event.capture());
        assertThat(event.getValue().getTables()).containsExactly(external);
        org.mockito.Mockito.verifyNoInteractions(dispatcher);
    }

    @Test
    void shouldReplaceTheIntentSharedBaselineWhenDiscoveringTables() throws Exception {
        final var source = source(DamengTestConfiguration.configuration().with("snapshot.locking.mode", "intent_shared").build());
        final var context = context();
        when(connection.getAllTableIds(null)).thenReturn(Set.of(EXISTING, ADDED));
        source.lockTablesForSchemaSnapshot(running, context);
        source.determineSnapshotOffset(context, null);
        assertThat(context.offset.getOffset().get("snapshot_baseline_lsn")).isEqualTo(100L);
        source.readTableStructure(running, context, null, new SnapshottingTask(true, false, List.of(), Map.of(), false));
        verify(connection).executeWithoutCommitting("LOCK TABLE \"APP\".\"EXISTING\" IN INTENT SHARE MODE");
        verify(connection).executeWithoutCommitting("LOCK TABLE \"APP\".\"ADDED\" IN INTENT SHARE MODE");
        assertThat(context.offset.getLsn()).isEqualTo(200);
        assertThat(context.offset.getOffset().get("snapshot_baseline_lsn")).isEqualTo(200L);
    }

    @ParameterizedTest
    @CsvSource({ "shared,SHARE", "intent_shared,INTENT SHARE" })
    void shouldHoldPrimaryLocksBeforeChoosingAReplayedStandbySnapshotBoundary(String mode, String sqlMode) throws Exception {
        final var source = source(sharedConfiguration().with("database.capture.role", "standby")
                .with("snapshot.locking.mode", mode).build());
        when(connection.requiresDatabaseLineage()).thenReturn(true);
        final var primary = mock(DamengConnection.class);
        final var standby = mock(DamengStandbySupport.class);
        when(standby.primary()).thenReturn(primary);
        when(primary.quotedTableIdString(EXISTING)).thenReturn(EXISTING.toDoubleQuotedString());
        when(standby.snapshotBoundary(connection)).thenReturn(432L);
        source.setStandbySupport(standby);
        final var context = context();
        source.lockTablesForSchemaSnapshot(running, context);
        source.determineSnapshotOffset(context, null);
        final var order = inOrder(primary, standby, connection);
        order.verify(primary).executeWithoutCommitting("LOCK TABLE \"APP\".\"EXISTING\" IN " + sqlMode + " MODE");
        order.verify(standby).snapshotBoundary(connection);
        order.verify(connection).validatePrimaryStandbyIdentity(context.offset);
        assertThat(context.offset.getLsn()).isEqualTo(432);
        verify(connection, never()).executeWithoutCommitting(any());
        verify(primary, never()).commit();
        doThrow(new SQLException("primary changed", "08006")).when(standby).validate(connection);
        assertThatThrownBy(() -> source.releaseDataSnapshotLocks(context)).isInstanceOf(SQLException.class).hasMessageContaining("primary changed");
    }

    @Test
    void shouldLogEachAcquisitionAndReleaseOnlyAfterTheTransactionRollsBack() throws Exception {
        final var nanos = new AtomicLong();
        clock = mock(Clock.class);
        when(clock.currentTimeInNanos()).thenAnswer(invocation -> nanos.get());
        final var jdbc = mock(Connection.class);
        final var source = executionSource(sharedConfiguration().build(), jdbc);
        when(connection.getAllTableIds(null)).thenReturn(Set.of(EXISTING, ADDED));
        doAnswer(invocation -> {
            assertThat(logs.containsMessage("Dameng snapshot acquiring shared lock on table")).isTrue();
            nanos.addAndGet(TimeUnit.MILLISECONDS.toNanos(25));
            return null;
        }).when(connection).executeWithoutCommitting(any());
        doAnswer(invocation -> {
            nanos.addAndGet(TimeUnit.MILLISECONDS.toNanos(100));
            return null;
        }).when(source).createSchemaChangeEventsForTables(any(), any(), any());
        doAnswer(invocation -> {
            assertThat(logs.containsMessage("shared table locks released")).isFalse();
            nanos.addAndGet(TimeUnit.MILLISECONDS.toNanos(10));
            return null;
        }).when(jdbc).rollback();

        execute(source);

        assertThat(logs.containsMessage("acquired shared lock on table APP.EXISTING in 25 ms (currently locked tables=2)")).isTrue();
        assertThat(logs.containsMessage("shared table locks released (tables=2, total acquisition time=50 ms, "
                + "maximum single-table acquisition time=25 ms, holding time=135 ms)")).isTrue();
        verify(jdbc).rollback();
    }

    @Test
    void shouldLogFailedAcquisitionAndNotReportSuccessfulRelease() throws Exception {
        final var source = executionSource(sharedConfiguration().build(), mock(Connection.class));
        doThrow(new SQLException("lock timed out")).when(connection).executeWithoutCommitting(any());

        assertThatThrownBy(() -> execute(source)).isInstanceOf(SQLException.class).hasMessage("lock timed out");

        assertThat(logs.containsWarnMessage("could not acquire shared lock on table APP.EXISTING")).isTrue();
        assertThat(logs.containsWarnMessage("currently locked tables=0")).isTrue();
        assertThat(logs.containsWarnMessage("table lock release is unconfirmed")).isTrue();
        assertThat(logs.containsMessage("shared table locks released")).isFalse();
    }

    @Test
    void shouldKeepObservedLocksAndWarnWhenReaderRollbackFails() throws Exception {
        final var raw = sharedConfiguration().build();
        final var metrics = lockMetrics(raw);
        final var jdbc = mock(Connection.class);
        final var source = executionSource(raw, jdbc);
        doThrow(new SQLException("connection lost")).when(jdbc).rollback();

        assertThatThrownBy(() -> execute(source)).isInstanceOf(RuntimeException.class).hasCauseInstanceOf(SQLException.class);

        assertThat(metrics.getSnapshotLocksHeld()).isTrue();
        assertThat(metrics.getLockedTableCount()).isEqualTo(1);
        assertThat(logs.containsWarnMessage("table lock release is unconfirmed")).isTrue();
        assertThat(logs.containsMessage("shared table locks released")).isFalse();
    }

    @Test
    void shouldKeepLastObservedLocksAfterBodyFailureEvenWhenRollbackSucceeds() throws Exception {
        final var raw = sharedConfiguration().build();
        final var metrics = lockMetrics(raw);
        final var jdbc = mock(Connection.class);
        final var source = executionSource(raw, jdbc);
        final var failure = new SQLException("schema read failed");
        doThrow(failure).when(source).readTableStructure(any(), any(), any(), any());

        assertThatThrownBy(() -> execute(source)).isSameAs(failure);

        verify(jdbc).rollback();
        assertThat(metrics.getSnapshotLocksHeld()).isTrue();
        assertThat(metrics.getLastSnapshotLockStatisticsAvailable()).isFalse();
        assertThat(logs.containsWarnMessage("table lock release is unconfirmed")).isTrue();
        assertThat(logs.containsMessage("shared table locks released")).isFalse();
    }

    @Test
    void shouldReleaseStandbySnapshotLocksOnlyAfterBothTransactionsRollBack() throws Exception {
        final var raw = sharedConfiguration().build();
        final var metrics = lockMetrics(raw);
        final var reader = mock(Connection.class);
        final var primary = mock(Connection.class);
        final var source = executionSource(raw, reader);
        standby(source, primary);
        doAnswer(invocation -> {
            assertThat(metrics.getSnapshotLocksHeld()).isTrue();
            assertThat(logs.containsMessage("shared table locks released")).isFalse();
            return null;
        }).when(primary).rollback();

        execute(source);

        final var order = inOrder(reader, primary);
        order.verify(reader).rollback();
        order.verify(primary).rollback();
        assertThat(metrics.getSnapshotLocksHeld()).isFalse();
        assertThat(metrics.getLastSnapshotLockedTableCount()).isEqualTo(1);
        assertThat(logs.containsMessage("shared table locks released")).isTrue();
    }

    @Test
    void shouldPreserveBodyAndReaderFailuresWhenPrimaryRollbackAlsoFails() throws Exception {
        final var reader = mock(Connection.class);
        final var primary = mock(Connection.class);
        final var source = executionSource(sharedConfiguration().build(), reader);
        standby(source, primary);
        final var bodyFailure = new SQLException("schema read failed");
        final var readerFailure = new SQLException("reader rollback failed");
        final var primaryFailure = new SQLException("primary rollback failed");
        doThrow(bodyFailure).when(source).readTableStructure(any(), any(), any(), any());
        doThrow(readerFailure).when(reader).rollback();
        doThrow(primaryFailure).when(primary).rollback();

        assertThatThrownBy(() -> execute(source)).hasCause(readerFailure)
                .satisfies(failure -> assertThat(failure.getSuppressed()).containsExactly(bodyFailure, primaryFailure));

        verify(primary).rollback();
        assertThat(logs.containsMessage("shared table locks released")).isFalse();
    }

    @Test
    void shouldFailTheSnapshotWhenOnlyPrimaryRollbackFails() throws Exception {
        final var reader = mock(Connection.class);
        final var primary = mock(Connection.class);
        final var source = executionSource(sharedConfiguration().build(), reader);
        standby(source, primary);
        final var failure = new SQLException("primary rollback failed");
        doThrow(failure).when(primary).rollback();

        assertThatThrownBy(() -> execute(source)).isSameAs(failure);

        verify(reader).rollback();
        assertThat(logs.containsWarnMessage("table lock release is unconfirmed")).isTrue();
    }

    @Test
    void shouldCleanUpTheSavedPrimaryConnectionWhenReaderInitializationFails() throws Exception {
        final var primary = mock(Connection.class);
        final var source = source(sharedConfiguration().build());
        final var primaryWrapper = standby(source, primary);
        when(primaryWrapper.connection()).thenReturn(primary).thenThrow(new SQLException("must not reconnect during cleanup"));
        final var failure = new SQLException("reader initialization failed");
        when(connection.connection()).thenThrow(failure);

        assertThatThrownBy(() -> execute(source)).isSameAs(failure);

        verify(primary).rollback();
        verify(primaryWrapper).connection();
    }

    @Test
    void shouldPreserveErrorsAndStillRollBackThePrimary() throws Exception {
        final var primary = mock(Connection.class);
        final var source = executionSource(sharedConfiguration().build(), mock(Connection.class));
        standby(source, primary);
        final var failure = new LinkageError("snapshot failed");
        doThrow(failure).when(source).readTableStructure(any(), any(), any(), any());
        doThrow(new SQLException("primary rollback failed")).when(primary).rollback();

        assertThatThrownBy(() -> execute(source)).isSameAs(failure)
                .satisfies(error -> assertThat(error.getSuppressed()).singleElement()
                        .satisfies(suppressed -> assertThat(suppressed).hasMessage("primary rollback failed")));
    }

    @ParameterizedTest
    @ValueSource(strings = { "initialization", "readerRollback", "primaryRollback" })
    void shouldCloseBothDeadlinesAfterFailureAndAllowAnotherSnapshot(String fault) throws Exception {
        final var reader = mock(Connection.class);
        final var primary = mock(Connection.class);
        final var source = executionSource(sharedConfiguration().with("snapshot.max.duration.ms", 500).build(), reader);
        standby(source, primary);
        final var expected = new SQLException(fault);
        switch (fault) {
            case "initialization" -> when(connection.connection()).thenThrow(expected).thenReturn(reader);
            case "readerRollback" -> doThrow(expected).doNothing().when(reader).rollback();
            case "primaryRollback" -> doThrow(expected).when(primary).rollback();
            default -> throw new AssertionError(fault);
        }
        final var firstFailure = new AtomicReference<Throwable>();
        final var unexpected = new AtomicReference<Throwable>();
        final var owner = new Thread(() -> {
            try {
                try {
                    execute(source);
                }
                catch (Exception e) {
                    firstFailure.set(e);
                }
                source.setStandbySupport(null);
                execute(source);
                // Old deadlines must not interrupt the next phase or abort either connection.
                new CountDownLatch(1).await(800, TimeUnit.MILLISECONDS);
            }
            catch (Throwable e) {
                unexpected.set(e);
            }
        }, "dameng-snapshot-reuse-test");

        owner.start();
        try {
            owner.join(TimeUnit.SECONDS.toMillis(5));
            assertThat(owner.isAlive()).isFalse();
            assertThat(firstFailure.get()).isNotNull();
            assertThat(unexpected.get()).isNull();
            verify(primary).rollback();
            verify(reader, times(fault.equals("initialization") ? 1 : 2)).rollback();
            verify(reader, never()).abort(any());
            verify(primary, never()).abort(any());
        }
        finally {
            owner.interrupt();
            owner.join(TimeUnit.SECONDS.toMillis(5));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void shouldKeepTheDeadlineActiveDuringPrimaryRollback(boolean rollbackFails) throws Exception {
        final var raw = sharedConfiguration().with("snapshot.max.duration.ms", 500).build();
        final var metrics = lockMetrics(raw);
        final var primary = mock(Connection.class);
        final var source = executionSource(raw, mock(Connection.class));
        standby(source, primary);
        final var rollbackStarted = new CountDownLatch(1);
        final var aborted = new CountDownLatch(1);
        final var failure = new AtomicReference<Throwable>();
        final var interruptedAfterCleanup = new AtomicBoolean();
        doAnswer(invocation -> {
            aborted.countDown();
            return null;
        }).when(primary).abort(any());
        doAnswer(invocation -> {
            rollbackStarted.countDown();
            // Model a JDBC driver that unblocks only when its connection is aborted.
            boolean finished = false;
            while (!finished) {
                try {
                    finished = aborted.await(5, TimeUnit.SECONDS);
                    assertThat(finished).isTrue();
                }
                catch (InterruptedException ignored) {
                }
            }
            if (rollbackFails) {
                throw new SQLException("rollback aborted");
            }
            return null;
        }).when(primary).rollback();
        final var owner = new Thread(() -> {
            try {
                execute(source);
            }
            catch (Throwable e) {
                failure.set(e);
            }
            finally {
                interruptedAfterCleanup.set(Thread.currentThread().isInterrupted());
            }
        }, "dameng-snapshot-cleanup-test");

        owner.start();
        try {
            assertThat(rollbackStarted.await(5, TimeUnit.SECONDS)).isTrue();
            owner.join(TimeUnit.SECONDS.toMillis(5));
            assertThat(owner.isAlive()).isFalse();
            assertThat(failure.get()).isInstanceOf(DamengSnapshotDeadline.SnapshotTimeoutException.class);
            if (rollbackFails) {
                assertThat(failure.get()).hasCauseInstanceOf(SQLException.class);
            }
            else {
                assertThat(failure.get().getCause()).isNull();
            }
            assertThat(interruptedAfterCleanup.get()).isFalse();
            assertThat(metrics.getSnapshotLocksHeld()).isTrue();
            assertThat(metrics.getLastSnapshotLockStatisticsAvailable()).isFalse();
            assertThat(logs.containsMessage("shared table locks released")).isFalse();
            verify(primary).abort(any());
        }
        finally {
            aborted.countDown();
            owner.interrupt();
            owner.join(TimeUnit.SECONDS.toMillis(5));
        }
    }

    @Test
    void shouldUseDefaultSharedLocksForNewlyDiscoveredTablesBeforeAdvancingSnapshotPosition() throws Exception {
        final var source = source(DamengTestConfiguration.configuration().with("table.include.list", "APP\\.EXISTING,APP\\.ADDED").build());
        final var context = context();
        when(connection.getAllTableIds(null)).thenReturn(Set.of(EXISTING, ADDED));

        source.lockTablesForSchemaSnapshot(running, context);
        source.determineSnapshotOffset(context, null);
        source.readTableStructure(running, context, null, new SnapshottingTask(true, true, List.of(), Map.of(), false));

        assertThat(context.capturedTables).containsExactly(ADDED, EXISTING);
        assertThat(context.offset.getLsn()).isEqualTo(200);
        assertThat(context.offset.getOffset()).doesNotContainKey("snapshot_baseline_lsn");
        final var order = inOrder(connection);
        order.verify(connection).executeWithoutCommitting("LOCK TABLE \"APP\".\"EXISTING\" IN SHARE MODE");
        order.verify(connection).currentLsn();
        order.verify(connection).getAllTableIds(null);
        order.verify(connection).executeWithoutCommitting("LOCK TABLE \"APP\".\"ADDED\" IN SHARE MODE");
        order.verify(connection).currentLsn();
        order.verify(connection).getAllTableIds(null);
        verify(listener).monitoredDataCollectionsDetermined(context.partition, context.capturedTables);
        assertThat(logs.containsWarnMessage("table.include.list rule")).isFalse();
    }

    @Test
    void shouldWarnAboutAnUndiscoveredConfiguredTableAndLogOnlySelectedTables() throws Exception {
        final var source = source(sharedConfiguration()
                .with("table.include.list", "APP\\.EXISTING,APP\\.MISSING").with("database.user", "CDC_USER").build());
        final var context = context();
        context.capturedSchemaTables.add(new TableId(null, "SYSDBA", "##PLAN_TABLE"));
        when(connection.getAllTableIds(null)).thenReturn(Set.of(EXISTING));

        source.determineSnapshotOffset(context, null);
        source.readTableStructure(running, context, null, new SnapshottingTask(true, true, List.of(), Map.of(), false));

        assertThat(logs.containsWarnMessage("table.include.list rule 'APP\\.MISSING' did not select any table visible to database user 'CDC_USER'")).isTrue();
        assertThat(logs.containsWarnMessage("table.include.list rule 'APP\\.EXISTING'")).isFalse();
        assertThat(logs.containsWarnMessage("metadata/SELECT privileges")).isTrue();
        assertThat(logs.containsMessage("Dameng tables selected for schema capture: [APP.EXISTING, SYSDBA.##PLAN_TABLE]")).isTrue();
        assertThat(logs.containsMessage("Dameng tables selected for data snapshot: [APP.EXISTING]")).isTrue();
        assertThat(context.offset.getLsn()).isEqualTo(100);
    }

    @Test
    void shouldWarnAboutUnmatchedFutureTablePatternsWithoutStoppingTheSnapshot() throws Exception {
        final var source = source(sharedConfiguration()
                .with("table.include.list", "APP\\.EXISTING,APP\\.FUTURE_.*").build());
        final var context = context();
        when(connection.getAllTableIds(null)).thenReturn(Set.of(EXISTING));

        source.determineSnapshotOffset(context, null);
        source.readTableStructure(running, context, null, new SnapshottingTask(true, true, List.of(), Map.of(), false));

        assertThat(logs.containsWarnMessage("table.include.list rule 'APP\\.FUTURE_.*'")).isTrue();
        assertThat(logs.containsWarnMessage("future tables matching the capture filters can still be captured")).isTrue();
        assertThat(context.capturedTables).containsExactly(EXISTING);
    }

    @Test
    void shouldUseFullNameMatchingWhenWarningAboutIncludePatterns() throws Exception {
        final var source = source(sharedConfiguration().with("table.include.list", "APP\\.EXIST").build());
        final var context = context();
        context.capturedTables.clear();
        context.capturedSchemaTables.clear();
        when(connection.getAllTableIds(null)).thenReturn(Set.of(EXISTING));

        source.determineSnapshotOffset(context, null);
        source.readTableStructure(running, context, null, new SnapshottingTask(true, true, List.of(), Map.of(), false));

        assertThat(logs.containsWarnMessage("table.include.list rule 'APP\\.EXIST'")).isTrue();
        assertThat(logs.containsMessage("Dameng tables selected for schema capture: [APP.EXISTING]")).isTrue();
        assertThat(logs.containsMessage("Dameng tables selected for data snapshot: []")).isTrue();
    }

    @Test
    void shouldPreserveCaseInsensitiveMatchingQuantifiersAndEscapedCommasInDiagnostics() throws Exception {
        final var table = new TableId(null, "APP", "ORDER,LOG12");
        final var source = source(sharedConfiguration().with("table.include.list", "app\\.order\\,log\\d{1\\,3}").build());
        final var context = context();
        context.capturedTables = new LinkedHashSet<>(Set.of(table));
        context.capturedSchemaTables = new LinkedHashSet<>(Set.of(table));
        when(connection.getAllTableIds(null)).thenReturn(Set.of(table));

        source.determineSnapshotOffset(context, null);
        source.readTableStructure(running, context, null, new SnapshottingTask(true, true, List.of(), Map.of(), false));

        assertThat(logs.containsWarnMessage("table.include.list rule")).isFalse();
        assertThat(logs.containsMessage("Dameng tables selected for schema capture: [APP.ORDER,LOG12]")).isTrue();
    }

    @Test
    void shouldNotWarnForSchemaTablesExcludedOnlyFromTheRowSnapshot() throws Exception {
        final var source = source(sharedConfiguration().with("table.include.list", "APP\\.EXISTING,APP\\.ADDED").build());
        final var context = context();
        context.capturedSchemaTables.add(ADDED);
        when(connection.getAllTableIds(null)).thenReturn(Set.of(EXISTING, ADDED));

        source.determineSnapshotOffset(context, null);
        source.readTableStructure(running, context, null, new SnapshottingTask(true, true, List.of("APP\\.EXISTING"), Map.of(), false));

        assertThat(logs.containsWarnMessage("table.include.list rule")).isFalse();
        assertThat(logs.containsMessage("Dameng tables selected for schema capture: [APP.ADDED, APP.EXISTING]")).isTrue();
        assertThat(logs.containsMessage("Dameng tables selected for data snapshot: [APP.EXISTING]")).isTrue();
    }

    @Test
    void shouldNotWarnForOtherConfiguredTablesDuringAnOnDemandSnapshot() throws Exception {
        final var source = source(sharedConfiguration().with("table.include.list", "APP\\.EXISTING,APP\\.ADDED").build());
        final var context = context();

        source.readTableStructure(running, context, null, new SnapshottingTask(true, true, List.of("APP\\.EXISTING"), Map.of(), true));

        assertThat(logs.containsWarnMessage("table.include.list rule")).isFalse();
        verify(connection, never()).getAllTableIds(null);
    }

    @Test
    void shouldLogNoDataTablesForASchemaOnlySnapshot() throws Exception {
        final var source = source(sharedConfiguration().build());
        final var context = context();
        when(connection.getAllTableIds(null)).thenReturn(Set.of(EXISTING));

        source.determineSnapshotOffset(context, null);
        source.readTableStructure(running, context, null, new SnapshottingTask(true, false, List.of(), Map.of(), false));

        assertThat(logs.containsMessage("Dameng tables selected for schema capture: [APP.EXISTING]")).isTrue();
        assertThat(logs.containsMessage("Dameng tables selected for data snapshot: []")).isTrue();
        assertThat(logs.containsWarnMessage("table.include.list rule")).isFalse();
    }

    @Test
    void shouldRepeatDiscoveryUntilNoAdditionalTableAppears() throws Exception {
        final var third = new TableId(null, "APP", "THIRD");
        final var source = source(sharedConfiguration().build());
        final var context = context();
        when(connection.getAllTableIds(null)).thenReturn(Set.of(EXISTING, ADDED), Set.of(EXISTING, ADDED, third));

        source.determineSnapshotOffset(context, null);
        source.readTableStructure(running, context, null, new SnapshottingTask(true, true, List.of(), Map.of(), false));

        assertThat(context.capturedTables).containsExactly(ADDED, EXISTING, third);
        assertThat(context.offset.getLsn()).isEqualTo(300);
        verify(connection).executeWithoutCommitting("LOCK TABLE \"APP\".\"ADDED\" IN SHARE MODE");
        verify(connection).executeWithoutCommitting("LOCK TABLE \"APP\".\"THIRD\" IN SHARE MODE");
        verify(connection, times(3)).getAllTableIds(null);
    }

    @Test
    void shouldRespectSnapshotAndTableFiltersDuringRediscovery() throws Exception {
        final var excluded = new TableId(null, "APP", "EXCLUDED");
        final var otherSnapshot = new TableId(null, "APP", "OTHER_SNAPSHOT");
        final var source = source(sharedConfiguration().with("table.exclude.list", "APP\\.EXCLUDED").build());
        final var context = context();
        when(connection.getAllTableIds(null)).thenReturn(Set.of(EXISTING, ADDED, excluded, otherSnapshot));

        source.determineSnapshotOffset(context, null);
        source.readTableStructure(running, context, null,
                new SnapshottingTask(true, true, List.of("APP\\.(EXISTING|ADDED|EXCLUDED)"), Map.of(), false));

        assertThat(context.capturedTables).containsExactly(ADDED, EXISTING);
        assertThat(context.capturedSchemaTables).containsExactlyInAnyOrder(ADDED, EXISTING, excluded, otherSnapshot);
        verify(connection).executeWithoutCommitting("LOCK TABLE \"APP\".\"EXCLUDED\" IN SHARE MODE");
        verify(connection).executeWithoutCommitting("LOCK TABLE \"APP\".\"OTHER_SNAPSHOT\" IN SHARE MODE");
    }

    @Test
    void shouldRediscoverSignalTableOutsideTheUserTableIncludeList() throws Exception {
        final var signal = new TableId(null, "APP", "SIGNAL");
        final var source = source(sharedConfiguration().with("table.include.list", "APP\\.EXISTING")
                .with("signal.data.collection", "APP.SIGNAL").build());
        final var context = context();
        when(connection.getAllTableIds(null)).thenReturn(Set.of(EXISTING, ADDED, signal));

        source.determineSnapshotOffset(context, null);
        source.readTableStructure(running, context, null, new SnapshottingTask(true, true, List.of(), Map.of(), false));

        assertThat(context.capturedTables).containsExactly(EXISTING, signal);
        verify(connection).executeWithoutCommitting("LOCK TABLE \"APP\".\"SIGNAL\" IN SHARE MODE");
        verify(connection).executeWithoutCommitting("LOCK TABLE \"APP\".\"ADDED\" IN SHARE MODE");
    }

    @Test
    void shouldKeepOriginalPositionWhenNoCapturedTableWasAdded() throws Exception {
        final var source = source(sharedConfiguration().build());
        final var context = context();
        when(connection.getAllTableIds(null)).thenReturn(Set.of(EXISTING));

        source.determineSnapshotOffset(context, null);
        source.readTableStructure(running, context, null, new SnapshottingTask(true, true, List.of(), Map.of(), false));

        assertThat(context.offset.getLsn()).isEqualTo(100);
        verify(connection).currentLsn();
        verify(connection, never()).executeWithoutCommitting(any());
    }

    @Test
    void shouldNotAdvanceARecoveredStreamingOffset() throws Exception {
        final var config = new DamengConnectorConfig(sharedConfiguration().build());
        final var previous = new DamengOffsetContext(config, 50);
        final var source = source(sharedConfiguration().build());
        final var context = context();

        source.determineSnapshotOffset(context, previous);
        source.readTableStructure(running, context, previous, new SnapshottingTask(true, false, List.of(), Map.of(), false));

        assertThat(context.offset).isSameAs(previous);
        verify(connection, never()).currentLsn();
        verify(connection, never()).getAllTableIds(null);
    }

    @Test
    void shouldAbortRediscoveryWhenTheConnectorStops() throws Exception {
        final var source = source(sharedConfiguration().build());
        final var context = context();
        source.determineSnapshotOffset(context, null);
        when(running.isRunning()).thenReturn(false);

        assertThatThrownBy(() -> source.readTableStructure(running, context, null, new SnapshottingTask(true, true, List.of(), Map.of(), false)))
                .isInstanceOf(InterruptedException.class);
        assertThat(context.offset.getLsn()).isEqualTo(100);
    }

    @Test
    void shouldDelegateSelectToTheConfiguredSnapshotQuery() {
        final var source = source(sharedConfiguration().build());
        final var query = mock(SnapshotQuery.class);
        final List<String> columns = List.of("\"ID\"", "\"NAME\"");
        when(snapshotterService.getSnapshotQuery()).thenReturn(query);
        when(query.snapshotQuery("\"APP\".\"EXISTING\"", columns)).thenReturn(Optional.of("SELECT custom_projection FROM custom_table"));

        assertThat(source.getSnapshotSelect(context(), EXISTING, columns)).contains("SELECT custom_projection FROM custom_table");
        verify(query).snapshotQuery("\"APP\".\"EXISTING\"", columns);
    }

    @Test
    void shouldUseSchemaTableSetWhenPersistingDefinitionsForNonSnapshottedTables() {
        final var source = source(sharedConfiguration().build());
        final var context = context();
        context.capturedSchemaTables = new LinkedHashSet<>(Set.of(EXISTING, ADDED));

        assertThat(source.getTablesForSchemaChange(context)).containsExactlyInAnyOrder(EXISTING, ADDED);
    }

    @Test
    void shouldKeepUncapturedSchemaTablesAndTheSignalTableByDefault() throws Exception {
        final var signal = new TableId(null, "APP", "SIGNAL");
        final var source = source(sharedConfiguration().with("table.include.list", "APP\\.EXISTING")
                .with("signal.data.collection", "APP.SIGNAL").build());
        final var context = context();
        context.capturedTables.add(signal);
        context.capturedSchemaTables.add(ADDED);

        assertThat(source.getTablesForSchemaChange(context)).containsExactly(ADDED, EXISTING, signal);
        source.lockTablesForSchemaSnapshot(running, context);

        verify(connection).executeWithoutCommitting("LOCK TABLE \"APP\".\"EXISTING\" IN SHARE MODE");
        verify(connection).executeWithoutCommitting("LOCK TABLE \"APP\".\"SIGNAL\" IN SHARE MODE");
        verify(connection).executeWithoutCommitting("LOCK TABLE \"APP\".\"ADDED\" IN SHARE MODE");
    }

    @Test
    void shouldLimitSchemaCaptureWhenOnlyCapturedTableHistoryIsRequested() {
        final var source = source(sharedConfiguration().with("table.include.list", "APP[.]EXISTING")
                .with("schema.history.internal.store.only.captured.tables.ddl", true).build());
        final var context = context();
        context.capturedSchemaTables.add(ADDED);

        assertThat(source.getTablesForSchemaChange(context)).containsExactly(EXISTING);
    }

    @Test
    void shouldKeepThePreviousPositionWhenRecoveringSchemasForNewlyVisibleTables() throws Exception {
        final var source = source(sharedConfiguration().with("snapshot.mode", "recovery").build());
        final var context = context();
        final var config = new DamengConnectorConfig(sharedConfiguration().build());
        final var checkpoint = new DamengOffsetContext(config, 40);
        checkpoint.setCommitLsn(70);
        checkpoint.setChangeLsn(65);
        checkpoint.setTransactionId("01");
        checkpoint.setEventSerialNo(3);
        checkpoint.validateDatabaseMagic(42);
        final var position = checkpoint.getOffset();
        when(connection.getAllTableIds(null)).thenReturn(Set.of(EXISTING, ADDED));

        source.determineSnapshotOffset(context, checkpoint);
        source.readTableStructure(running, context, checkpoint, new SnapshottingTask(true, false, List.of(), Map.of(), false));

        assertThat(context.offset).isSameAs(checkpoint);
        assertThat(context.offset.getOffset()).isEqualTo(position);
        verify(connection, never()).currentLsn();
    }

    @Test
    void shouldLockTablesExcludedFromTheRowSnapshotBeforeReadingTheOffset() throws Exception {
        final var source = source(sharedConfiguration().build());
        final var context = context();
        context.capturedSchemaTables.add(ADDED);

        source.lockTablesForSchemaSnapshot(running, context);
        source.determineSnapshotOffset(context, null);

        final var order = inOrder(connection);
        order.verify(connection).executeWithoutCommitting("LOCK TABLE \"APP\".\"ADDED\" IN SHARE MODE");
        order.verify(connection).executeWithoutCommitting("LOCK TABLE \"APP\".\"EXISTING\" IN SHARE MODE");
        order.verify(connection).currentLsn();
        assertThat(context.capturedTables).containsExactly(EXISTING);
    }

    @Test
    void shouldRejectSnapshotCompletionAfterPrimaryChanges() throws Exception {
        final var source = source(sharedConfiguration().build());
        final var context = context();
        source.determineSnapshotOffset(context, null);
        context.offset.preSnapshotStart(false);
        doThrow(new SQLException("Primary changed during the snapshot", "08006")).when(connection).validateUnchangedPrimary();

        assertThatThrownBy(() -> source.releaseDataSnapshotLocks(context))
                .isInstanceOf(SQLException.class).hasMessageContaining("Primary changed");
        assertThat(context.offset.getOffset().get("snapshot_completed")).isEqualTo(false);
    }

    @Test
    void shouldValidatePrimaryAtTheSnapshotCompletionBoundary() throws Exception {
        final var source = source(sharedConfiguration().build());
        final var context = context();
        source.determineSnapshotOffset(context, null);
        context.offset.preSnapshotStart(false);

        source.releaseDataSnapshotLocks(context);
        context.offset.preSnapshotCompletion();

        verify(connection).validateUnchangedPrimary();
        assertThat(context.offset.getOffset().get("snapshot_completed")).isEqualTo(true);
    }

    @Test
    void shouldDiscoverSchemaOnlyTablesWithoutAddingTheirRowsToTheSnapshot() throws Exception {
        final var source = source(sharedConfiguration().build());
        final var context = context();
        when(connection.getAllTableIds(null)).thenReturn(Set.of(EXISTING, ADDED));

        source.determineSnapshotOffset(context, null);
        source.readTableStructure(running, context, null,
                new SnapshottingTask(true, true, List.of("APP\\.EXISTING"), Map.of(), false));

        assertThat(context.capturedTables).containsExactly(EXISTING);
        assertThat(context.capturedSchemaTables).containsExactlyInAnyOrder(EXISTING, ADDED);
        assertThat(context.offset.getLsn()).isEqualTo(200);
        final var order = inOrder(connection);
        order.verify(connection).currentLsn();
        order.verify(connection).getAllTableIds(null);
        order.verify(connection).executeWithoutCommitting("LOCK TABLE \"APP\".\"ADDED\" IN SHARE MODE");
        order.verify(connection).currentLsn();
        order.verify(connection).getAllTableIds(null);
        verify(listener, never()).monitoredDataCollectionsDetermined(any(), any());
    }

    @Test
    void shouldReadSchemaForTablesExcludedFromTheDataSnapshot() throws Exception {
        final var source = source(sharedConfiguration().with("column.exclude.list", "APP\\.ADDED\\.SECRET").build());
        final var context = context();
        context.capturedSchemaTables = new LinkedHashSet<>(Set.of(EXISTING, ADDED));
        context.tables.overwriteTable(Table.editor().tableId(ADDED)
                .addColumn(Column.editor().name("ID").jdbcType(Types.INTEGER).position(1).create())
                .addColumn(Column.editor().name("SECRET").jdbcType(Types.VARCHAR).position(2).create())
                .addColumn(Column.editor().name("VALUE").jdbcType(Types.VARCHAR).position(3).create()).create());
        when(connection.getTableObjectId(ADDED)).thenReturn(123L);

        source.readTableStructure(running, context, null, new SnapshottingTask(true, true, List.of(), Map.of(), true));

        final ArgumentCaptor<TableFilter> filter = ArgumentCaptor.forClass(TableFilter.class);
        verify(connection).readSchema(any(), isNull(), eq("APP"), filter.capture(), isNull(), eq(false));
        assertThat(filter.getValue().isIncluded(EXISTING)).isTrue();
        assertThat(filter.getValue().isIncluded(ADDED)).isTrue();
        assertThat(context.tables.forTable(ADDED).attributeWithName("dameng.object.id").asLong()).isEqualTo(123);
        assertThat(context.tables.forTable(ADDED).columns()).extracting(Column::name).containsExactly("ID", "SECRET", "VALUE");
        assertThat(context.tables.forTable(ADDED).columnWithName("VALUE").position()).isEqualTo(3);
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldUseConfiguredDatabaseNameForInitialSnapshotSchemaChanges() {
        final var configuration = sharedConfiguration()
                .with("database.dbname", "DM_TEST")
                .with("include.schema.changes", true)
                .build();
        final var source = source(configuration);
        final var context = (RelationalSnapshotContext<DamengPartition, DamengOffsetContext>) source.prepare(new DamengPartition("dameng"), false);
        context.offset = new DamengOffsetContext(new DamengConnectorConfig(configuration), 100L);
        context.offset.event(EXISTING, Instant.now());
        final var table = Table.editor().tableId(EXISTING)
                .addColumn(Column.editor().name("ID").jdbcType(Types.INTEGER).position(1).create())
                .create();

        final var event = source.getCreateTableEvent(context, table);

        assertThat(event.getDatabase()).isEqualTo("DM_TEST");
        assertThat(event.getSource().getString("db")).isEqualTo("DM_TEST");
        assertThat(event.getSchema()).isEqualTo("APP");
        assertThat(event.isFromSnapshot()).isTrue();
        assertThat(table.id().catalog()).isNull();
    }

    @Test
    void shouldHonorACustomSnapshotQueryThatSkipsATable() {
        final var source = source(sharedConfiguration().build());
        final var query = mock(SnapshotQuery.class);
        when(snapshotterService.getSnapshotQuery()).thenReturn(query);
        when(query.snapshotQuery("\"APP\".\"EXISTING\"", List.of("\"ID\""))).thenReturn(Optional.empty());

        assertThat(source.getSnapshotSelect(context(), EXISTING, List.of("\"ID\""))).isEmpty();
    }

    private void execute(DamengSnapshotChangeEventSource source) throws Exception {
        source.doExecute(running, null, context(), new SnapshottingTask(true, false, List.of(), Map.of(), false));
    }

    private DamengSnapshotChangeEventSource executionSource(Configuration raw, Connection jdbc) throws Exception {
        when(connection.connection()).thenReturn(jdbc);
        when(connection.isValid()).thenReturn(true);
        when(connection.getAllTableIds(null)).thenReturn(Set.of(EXISTING));
        final var source = spy(source(raw));
        // Schema event publication is outside these transaction lifecycle tests.
        doNothing().when(source).createSchemaChangeEventsForTables(any(), any(), any());
        return source;
    }

    private DamengConnection standby(DamengSnapshotChangeEventSource source, Connection primary) throws Exception {
        final var primaryWrapper = mock(DamengConnection.class);
        when(primaryWrapper.connection()).thenReturn(primary);
        final var standby = mock(DamengStandbySupport.class);
        when(standby.primary()).thenReturn(primaryWrapper);
        when(standby.snapshotBoundary(connection)).thenReturn(100L);
        source.setStandbySupport(standby);
        return primaryWrapper;
    }

    private DamengSnapshotChangeEventSourceMetrics lockMetrics(Configuration raw) {
        final var config = new DamengConnectorConfig(raw);
        final var metrics = new DamengSnapshotChangeEventSourceMetrics(new DamengTaskContext(raw, config),
                mock(ChangeEventQueueMetrics.class), new DamengEventMetadataProvider());
        listener = metrics;
        return metrics;
    }

    private static Configuration.Builder sharedConfiguration() {
        return DamengTestConfiguration.configuration().with("snapshot.locking.mode", "shared");
    }

    @SuppressWarnings("unchecked")
    private DamengSnapshotChangeEventSource source(Configuration configuration) {
        final var config = new DamengConnectorConfig(configuration);
        if (config.getSnapshotLockingMode().orElseThrow() == DamengConnectorConfig.SnapshotLockingMode.INTENT_SHARED) {
            when(snapshotterService.getSnapshotLock()).thenReturn(new io.debezium.connector.dameng.snapshot.IntentSharedSnapshotLock());
        }
        final MainConnectionProvidingConnectionFactory<DamengConnection> factory = mock(MainConnectionProvidingConnectionFactory.class);
        when(factory.mainConnection()).thenReturn(connection);
        return new DamengSnapshotChangeEventSource(config, factory, mock(DamengDatabaseSchema.class),
                mock(EventDispatcher.class), clock, listener, new NotificationService<>(List.of(), config, null, ignored -> {
                }), snapshotterService);
    }

    private RelationalSnapshotContext<DamengPartition, DamengOffsetContext> context() {
        final var context = new RelationalSnapshotContext<DamengPartition, DamengOffsetContext>(new DamengPartition("dameng"), null, false);
        context.capturedTables = new LinkedHashSet<>(Set.of(EXISTING));
        context.capturedSchemaTables = new LinkedHashSet<>(Set.of(EXISTING));
        return context;
    }
}
