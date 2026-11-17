/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.jdbc.MainConnectionProvidingConnectionFactory;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.source.SnapshottingTask;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.spi.SnapshotResult;
import io.debezium.relational.Attribute;
import io.debezium.relational.RelationalSnapshotChangeEventSource;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.schema.SchemaChangeEvent;
import io.debezium.snapshot.SnapshotterService;
import io.debezium.util.Clock;
import io.debezium.util.Strings;

public final class DamengSnapshotChangeEventSource extends RelationalSnapshotChangeEventSource<DamengPartition, DamengOffsetContext> {
    private static final Logger LOGGER = LoggerFactory.getLogger(DamengSnapshotChangeEventSource.class);
    private final DamengConnectorConfig config;
    private final DamengConnection connection;
    private final SnapshotProgressListener<DamengPartition> listener;
    private final SnapshotterService snapshotterService;
    private final DamengSnapshotLockMeter locks;
    private final DamengDatabaseSchema damengSchema;
    private DamengStandbySupport standby;
    private final Map<TableId, DamengTableType.Identity> nonRowTables = new HashMap<>();

    void setStandbySupport(DamengStandbySupport standby) {
        this.standby = standby;
    }

    public DamengSnapshotChangeEventSource(DamengConnectorConfig config, MainConnectionProvidingConnectionFactory<DamengConnection> connectionFactory,
                                           DamengDatabaseSchema schema, EventDispatcher<DamengPartition, TableId> dispatcher, Clock clock,
                                           SnapshotProgressListener<DamengPartition> listener,
                                           NotificationService<DamengPartition, DamengOffsetContext> notifications,
                                           SnapshotterService snapshotterService) {
        super(config, connectionFactory, schema, dispatcher, clock, listener, notifications, snapshotterService);
        this.damengSchema = schema;
        this.config = config;
        this.connection = connectionFactory.mainConnection();
        this.listener = listener;
        this.snapshotterService = snapshotterService;
        this.locks = listener instanceof DamengSnapshotChangeEventSourceMetrics metrics
                ? metrics.lockMeter()
                : new DamengSnapshotLockMeter(clock);
    }

    @Override
    protected void createSchemaChangeEventsForTables(ChangeEventSourceContext sourceContext,
                                                     RelationalSnapshotContext<DamengPartition, DamengOffsetContext> context,
                                                     SnapshottingTask snapshottingTask)
            throws Exception {
        tryStartingSnapshot(context);
        for (TableId id : schemaTables(context)) {
            final var table = context.tables.forTable(id);
            if (DamengTableType.isNonRow(table)) {
                if (!sourceContext.isRunning()) {
                    throw new InterruptedException("Interrupted while recording non-row table " + id);
                }
                context.offset.event(id, clock.currentTimeAsInstant());
                damengSchema.applySchemaChange(getCreateTableEvent(context, table));
            }
        }
        super.createSchemaChangeEventsForTables(sourceContext, context, snapshottingTask);
    }

    @Override
    protected SnapshotContext<DamengPartition, DamengOffsetContext> prepare(DamengPartition partition, boolean onDemand) {
        nonRowTables.clear();
        return new DamengSnapshotContext(partition, config.getDatabaseName(), onDemand);
    }

    @Override
    public SnapshotResult<DamengOffsetContext> doExecute(ChangeEventSourceContext context, DamengOffsetContext previousOffset,
                                                         SnapshotContext<DamengPartition, DamengOffsetContext> snapshotContext,
                                                         SnapshottingTask snapshottingTask)
            throws Exception {
        Connection primarySnapshotConnection = null;
        DamengSnapshotDeadline deadline = null;
        DamengSnapshotDeadline primaryDeadline = null;
        SnapshotResult<DamengOffsetContext> result = null;
        Throwable failure = null;
        try {
            if (standby != null) {
                primarySnapshotConnection = standby.primary().connection();
                primaryDeadline = new DamengSnapshotDeadline(primarySnapshotConnection, config.getSnapshotMaxDurationMs());
            }
            deadline = new DamengSnapshotDeadline(connection.connection(), config.getSnapshotMaxDurationMs());
            result = super.doExecute(context, previousOffset, snapshotContext, snapshottingTask);
        }
        catch (Exception | Error e) {
            failure = e;
        }
        finally {
            try {
                // The parent owns the reader transaction. Standby snapshots also
                // hold locks on this saved primary connection until execution ends.
                if (primarySnapshotConnection != null) {
                    primarySnapshotConnection.rollback();
                }
            }
            catch (Exception | Error e) {
                if (failure == null) {
                    failure = e;
                }
                else if (failure != e) {
                    failure.addSuppressed(e);
                }
            }
            finally {
                // Keep both deadlines active through rollback, then prevent an old
                // timer from aborting a connection used by streaming or another snapshot.
                if (deadline != null) {
                    deadline.close();
                }
                if (primaryDeadline != null) {
                    primaryDeadline.close();
                }
            }
        }
        if ((deadline != null && deadline.expired()) || (primaryDeadline != null && primaryDeadline.expired())) {
            Thread.interrupted();
            failure = new DamengSnapshotDeadline.SnapshotTimeoutException(config.getSnapshotMaxDurationMs(), failure);
        }
        if (failure != null) {
            LOGGER.warn("Dameng snapshot did not complete successfully; table lock release is unconfirmed "
                    + "(last observed locked tables={}, acquisition time={} ms, elapsed holding time={} ms). Check the database session.",
                    locks.lockedTableCount(), locks.waitDurationMillis(), locks.holdDurationMillis(), failure);
            if (failure instanceof Exception exception) {
                throw exception;
            }
            throw (Error) failure;
        }
        locks.locksReleased();
        if (locks.resultAvailable()) {
            LOGGER.info("Dameng snapshot transaction rolled back; {} table locks released "
                    + "(tables={}, total acquisition time={} ms, maximum single-table acquisition time={} ms, holding time={} ms)",
                    config.getSnapshotLockingMode().orElseThrow().getValue(), locks.lastLockedTableCount(), locks.lastWaitDurationMillis(),
                    locks.lastMaxWaitDurationMillis(), locks.lastHoldDurationMillis());
        }
        return result;
    }

    @Override
    protected Set<TableId> getAllTableIds(RelationalSnapshotContext<DamengPartition, DamengOffsetContext> context) throws SQLException {
        nonRowTables.putAll(connection.readNonRowTableIds());
        return connection.getAllTableIds(null);
    }

    @Override
    protected void lockTablesForSchemaSnapshot(ChangeEventSourceContext sourceContext,
                                               RelationalSnapshotContext<DamengPartition, DamengOffsetContext> context)
            throws SQLException, InterruptedException {
        lockTables(sourceContext, schemaTables(context));
    }

    private void lockTables(ChangeEventSourceContext sourceContext, Collection<TableId> tables) throws SQLException, InterruptedException {
        final var currentNonRow = connection.readNonRowTableIds();
        final var lockMode = config.getSnapshotLockingMode().orElseThrow().getValue();
        for (TableId table : tables) {
            if (!sourceContext.isRunning()) {
                throw new InterruptedException("Interrupted while locking " + table);
            }
            final var identity = currentNonRow.get(table);
            if (identity != null) {
                nonRowTables.put(table, identity);
                continue;
            }
            // Use the configured statement timeout without committing the transaction
            // that must hold these locks until the snapshot has finished.
            LOGGER.info("Dameng snapshot acquiring {} lock on table {} (currently locked tables={})", lockMode, table, locks.lockedTableCount());
            locks.lockWaitStarted();
            try {
                final var lockConnection = standby == null ? connection : standby.primary();
                lockConnection.executeWithoutCommitting(snapshotterService.getSnapshotLock()
                        .tableLockingStatement(config.snapshotLockTimeout(), lockConnection.quotedTableIdString(table)).orElseThrow());
            }
            catch (SQLException | RuntimeException | Error e) {
                final long elapsedMillis = locks.lockWaitFinished(false);
                LOGGER.warn("Dameng snapshot could not acquire {} lock on table {} after {} ms (currently locked tables={}): {}",
                        lockMode, table, elapsedMillis, locks.lockedTableCount(), e.getMessage());
                if (standby != null && e instanceof SQLException) {
                    standby.validate(connection);
                }
                throw e;
            }
            final long elapsedMillis = locks.lockWaitFinished(true);
            LOGGER.info("Dameng snapshot acquired {} lock on table {} in {} ms (currently locked tables={})",
                    lockMode, table, elapsedMillis, locks.lockedTableCount());
        }
    }

    @Override
    protected void releaseSchemaSnapshotLocks(RelationalSnapshotContext<DamengPartition, DamengOffsetContext> context) throws SQLException {
        // Keep the selected table locks until the snapshot finishes. The base class rolls
        // back this transaction after data and schema have been read, including on failure.
    }

    @Override
    protected void releaseDataSnapshotLocks(RelationalSnapshotContext<DamengPartition, DamengOffsetContext> context) throws SQLException {
        // The base class calls this before publishing the completed snapshot offset.
        // A primary change during the scan must not be acknowledged as a complete snapshot.
        connection.validateUnchangedPrimary();
        connection.validateUnchangedCompatibility();
        if (standby != null) {
            standby.validate(connection);
        }
    }

    @Override
    protected void determineSnapshotOffset(RelationalSnapshotContext<DamengPartition, DamengOffsetContext> context,
                                           DamengOffsetContext previousOffset)
            throws Exception {
        if (previousOffset != null && (config.getSnapshotMode() == DamengConnectorConfig.SnapshotMode.RECOVERY
                || !previousOffset.isInitialSnapshotRunning())) {
            context.offset = previousOffset;
        }
        else {
            context.offset = newSnapshotOffset();
        }
        validateDatabaseIdentity(context.offset);
    }

    private DamengOffsetContext newSnapshotOffset() throws SQLException {
        final var offset = new DamengOffsetContext(config, snapshotBoundary());
        if (config.getSnapshotLockingMode().orElseThrow() == DamengConnectorConfig.SnapshotLockingMode.INTENT_SHARED) {
            offset.setSnapshotBaselineLsn(offset.getLsn());
        }
        return offset;
    }

    @Override
    protected void readTableStructure(ChangeEventSourceContext sourceContext,
                                      RelationalSnapshotContext<DamengPartition, DamengOffsetContext> context,
                                      DamengOffsetContext offsetContext, SnapshottingTask task)
            throws SQLException, InterruptedException {
        if (!task.isOnDemand() && config.getSnapshotMode() != DamengConnectorConfig.SnapshotMode.RECOVERY
                && (offsetContext == null || offsetContext.isInitialSnapshotRunning())) {
            includeTablesCreatedDuringSnapshotPreparation(sourceContext, context, task);
        }
        // Tables omitted by the snapshot row filter still need their historical
        // column layout and object identity for subsequent dictionary-free redo.
        final Set<TableId> schemaTables = schemaTables(context);
        final var currentNonRow = connection.readNonRowTableIds();
        for (TableId id : schemaTables) {
            if (!java.util.Objects.equals(nonRowTables.get(id), currentNonRow.get(id))) {
                throw new SQLException("Non-row table identity changed across the Dameng snapshot boundary for " + id + "; retry the snapshot");
            }
        }
        // Non-row definitions remain in history, but never enter row scans or JDBC column conversion.
        context.capturedSchemaTables = schemaTables;
        if (context.capturedTables.stream().anyMatch(nonRowTables::containsKey)) {
            context.capturedTables.stream().filter(nonRowTables::containsKey)
                    .forEach(id -> listener.dataCollectionSnapshotCompleted(context.partition, id, 0));
            context.capturedTables = context.capturedTables.stream().filter(id -> !nonRowTables.containsKey(id))
                    .collect(Collectors.toCollection(LinkedHashSet::new));
        }
        logCaptureSelection(context, schemaTables, task);
        for (String schemaName : schemaTables.stream().map(TableId::schema).distinct().toList()) {
            if (!sourceContext.isRunning()) {
                throw new InterruptedException("Interrupted while reading schema " + schemaName);
            }
            // Keep excluded columns in the historical model: COL n identifies the
            // physical ordinal, even when a column is omitted from emitted events.
            connection.readSchema(context.tables, null, schemaName, id -> schemaTables.contains(id) && !nonRowTables.containsKey(id), null, false);
        }
        for (TableId tableId : schemaTables) {
            if (nonRowTables.containsKey(tableId)) {
                context.tables.overwriteTable(nonRowTables.get(tableId).table(tableId));
                continue;
            }
            final Table table = context.tables.forTable(tableId);
            if (table != null) {
                context.tables.overwriteTable(table.edit().addAttributes(table.attributes()).addAttribute(Attribute.editor().name("dameng.object.id")
                        .value(Long.toString(connection.getTableObjectId(tableId))).create()).create());
            }
        }
    }

    private void logCaptureSelection(RelationalSnapshotContext<DamengPartition, DamengOffsetContext> context,
                                     Set<TableId> schemaTables, SnapshottingTask task) {
        LOGGER.info("Dameng tables selected for schema capture: {}", schemaTables);
        LOGGER.info("Dameng tables selected for data snapshot: {}",
                task.snapshotData() ? context.capturedTables.stream().sorted().toList() : List.of());
        if (!task.isOnDemand()) {
            // Use the same splitting, full-name matching and case handling as the table filter.
            // An unmatched rule may refer to a future table; discovery alone cannot prove a privilege failure.
            for (Pattern pattern : Strings.setOfRegex(config.tableIncludeList(), Pattern.CASE_INSENSITIVE)) {
                if (schemaTables.stream().noneMatch(table -> pattern.matcher(config.getTableIdMapper().toString(table)).matches())) {
                    LOGGER.warn("Dameng table.include.list rule '{}' did not select any table visible to database user '{}'. "
                            + "Check table existence, identifier case, metadata/SELECT privileges, and database/schema filters. "
                            + "This rule contributes no table to this snapshot; future tables matching the capture filters can still be captured.",
                            pattern.pattern(), config.getJdbcConfig().getString("user"));
                }
            }
        }
    }

    private void includeTablesCreatedDuringSnapshotPreparation(ChangeEventSourceContext sourceContext,
                                                               RelationalSnapshotContext<DamengPartition, DamengOffsetContext> context,
                                                               SnapshottingTask task)
            throws SQLException, InterruptedException {
        final var snapshotPatterns = getDataCollectionPattern(task.getDataCollections());
        while (sourceContext.isRunning()) {
            // The offset was read before this enumeration. Any table created after
            // enumeration will therefore have a CREATE record in the streaming range.
            // A table discovered here must be locked before choosing a newer offset.
            final Set<TableId> allTables = connection.getAllTableIds(null);
            final Set<TableId> schemaTables = schemaTables(context);
            final Set<TableId> addedSchemaTables = allTables.stream()
                    .filter(this::isSchemaTableIncluded)
                    .filter(tableId -> !schemaTables.contains(tableId))
                    .sorted().collect(Collectors.toCollection(LinkedHashSet::new));
            final Set<TableId> addedDataTables = determineDataCollectionsToBeSnapshotted(allTables, snapshotPatterns)
                    .filter(config.getTableFilters().dataCollectionFilter()::isIncluded)
                    .filter(tableId -> !context.capturedTables.contains(tableId))
                    .sorted().collect(Collectors.toCollection(LinkedHashSet::new));
            if (addedSchemaTables.isEmpty() && addedDataTables.isEmpty()) {
                return;
            }
            if (!addedSchemaTables.isEmpty()) {
                lockTables(sourceContext, addedSchemaTables);
            }
            final Set<TableId> captured = new LinkedHashSet<>(context.capturedTables);
            captured.addAll(addedDataTables);
            context.capturedTables = captured.stream().sorted().collect(Collectors.toCollection(LinkedHashSet::new));
            schemaTables.addAll(addedSchemaTables);
            context.capturedSchemaTables = schemaTables;
            if (!addedSchemaTables.isEmpty()) {
                context.offset = newSnapshotOffset();
                validateDatabaseIdentity(context.offset);
            }
            if (!addedDataTables.isEmpty()) {
                listener.monitoredDataCollectionsDetermined(context.partition, context.capturedTables);
            }
        }
        throw new InterruptedException("Interrupted while checking for new Dameng snapshot tables");
    }

    @Override
    protected Collection<TableId> getTablesForSchemaChange(RelationalSnapshotContext<DamengPartition, DamengOffsetContext> context) {
        return schemaTables(context).stream().filter(id -> !DamengTableType.isNonRow(context.tables.forTable(id))).toList();
    }

    private Set<TableId> schemaTables(RelationalSnapshotContext<DamengPartition, DamengOffsetContext> context) {
        final Set<TableId> tables = new LinkedHashSet<>(context.capturedTables);
        if (context.capturedSchemaTables != null) {
            context.capturedSchemaTables.stream().filter(this::isSchemaTableIncluded).forEach(tables::add);
        }
        return tables.stream().sorted().collect(Collectors.toCollection(LinkedHashSet::new));
    }

    private boolean isSchemaTableIncluded(TableId tableId) {
        return !config.isInternalTableExcluded(tableId)
                && (config.storeOnlyCapturedTables() ? config.getTableFilters().dataCollectionFilter().isIncluded(tableId)
                        : config.getTableFilters().eligibleForSchemaDataCollectionFilter().isIncluded(tableId));
    }

    private void validateDatabaseIdentity(DamengOffsetContext offset) throws SQLException {
        connection.validateCompatibilityEnvironment(offset);
        if (connection.requiresDatabaseLineage()) {
            connection.validatePrimaryStandbyIdentity(offset);
        }
        else {
            offset.validateDatabaseMagic(connection.databaseMagic());
        }
    }

    private long snapshotBoundary() throws SQLException {
        return standby == null ? connection.currentLsn() : standby.snapshotBoundary(connection);
    }

    @Override
    protected SchemaChangeEvent getCreateTableEvent(RelationalSnapshotContext<DamengPartition, DamengOffsetContext> context, Table table) {
        return SchemaChangeEvent.ofSnapshotCreate(context.partition, context.offset, context.catalogName, table);
    }

    @Override
    protected Optional<String> getSnapshotSelect(RelationalSnapshotContext<DamengPartition, DamengOffsetContext> context,
                                                 TableId tableId, List<String> columns) {
        return snapshotterService.getSnapshotQuery().snapshotQuery(connection.quotedTableIdString(tableId), columns);
    }

    @Override
    protected DamengOffsetContext copyOffset(RelationalSnapshotContext<DamengPartition, DamengOffsetContext> context) {
        return new DamengOffsetContext.Loader(config).load(context.offset.getOffset());
    }

    private static final class DamengSnapshotContext extends RelationalSnapshotContext<DamengPartition, DamengOffsetContext> {
        private DamengSnapshotContext(DamengPartition partition, String databaseName, boolean onDemand) {
            super(partition, databaseName, onDemand);
        }
    }
}
