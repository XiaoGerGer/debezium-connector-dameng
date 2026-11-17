/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import static io.debezium.config.CommonConnectorConfig.WatermarkStrategy.INSERT_DELETE;

import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.signal.actions.snapshotting.OpenIncrementalSnapshotWindow;
import io.debezium.pipeline.source.snapshot.incremental.DeleteWindowCloser;
import io.debezium.pipeline.source.snapshot.incremental.InsertWindowCloser;
import io.debezium.pipeline.source.snapshot.incremental.SignalBasedIncrementalSnapshotChangeEventSource;
import io.debezium.pipeline.source.snapshot.incremental.SignalMetadata;
import io.debezium.pipeline.source.snapshot.incremental.WatermarkWindowCloser;
import io.debezium.pipeline.source.spi.DataChangeEventListener;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.Attribute;
import io.debezium.relational.SnapshotChangeRecordEmitter;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.schema.DataCollectionSchema;
import io.debezium.util.Clock;

/** Uses independent snapshot transactions, routing standby watermarks to the primary. */
public final class DamengIncrementalSnapshotChangeEventSource extends SignalBasedIncrementalSnapshotChangeEventSource<DamengPartition, TableId> {
    private static final Logger LOGGER = LoggerFactory.getLogger(DamengIncrementalSnapshotChangeEventSource.class);
    private final DamengConnectorConfig config;
    private final DamengConnection connection;
    private final Clock clock;
    private final DamengIncrementalSnapshotMeter meter;
    private DamengStandbySupport standby;
    private SignalMetadata standbySignalMetadata;

    void setStandbySupport(DamengStandbySupport standby) {
        this.standby = standby;
    }

    @Override
    public void init(DamengPartition partition, OffsetContext offset) {
        // An initialization always rereads the interrupted chunk. Never retain
        // rows buffered by a previous initialization, including removed tables.
        window.clear();
        if (offset instanceof DamengOffsetContext damengOffset) {
            pruneExcludedSnapshotTables(config, damengOffset);
        }
        super.init(partition, offset);
    }

    /**
     * Removes excluded tables before resuming an incremental snapshot without
     * advancing the CDC position. The next emitted offset persists the queue.
     */
    static List<TableId> pruneExcludedSnapshotTables(DamengConnectorConfig config, DamengOffsetContext offset) {
        if (offset == null) {
            return List.of();
        }
        final var snapshot = offset.getIncrementalSnapshotContext();
        if (!snapshot.snapshotRunning()) {
            return List.of();
        }
        final List<TableId> excluded = snapshot.getDataCollections().stream()
                .map(collection -> collection.getId())
                .filter(table -> !config.getTableFilters().dataCollectionFilter().isIncluded(table))
                .distinct().toList();
        if (excluded.isEmpty()) {
            return excluded;
        }
        final boolean currentExcluded = excluded.contains(snapshot.currentDataCollectionId().getId());
        if (currentExcluded) {
            // nextDataCollection resets the old table's last key, maximum key
            // and schema. Merely removing its queue entry would reuse its keys
            // for the next table and silently omit rows from that table.
            snapshot.nextDataCollection();
            snapshot.revertChunk();
            // Old source-channel watermarks may be replayed after restart.
            // Invalidate them even when the remaining snapshot is paused.
            snapshot.startNewChunk();
        }
        for (TableId table : excluded) {
            snapshot.removeDataCollectionFromSnapshot(table.toDoubleQuotedString());
        }
        if (!snapshot.snapshotRunning()) {
            snapshot.unsetCorrelationId();
            if (snapshot.isSnapshotPaused()) {
                snapshot.resumeSnapshot();
            }
        }
        LOGGER.info("Dameng capture scope removed tables {} from the pending incremental snapshot (current table removed={}, remaining tables={})",
                excluded, currentExcluded, snapshot.dataCollectionsToBeSnapshottedCount());
        return excluded;
    }

    @Override
    protected void readChunk(DamengPartition partition, OffsetContext offset) throws InterruptedException {
        if (context != null && context.snapshotRunning()) {
            if (!context.isSnapshotPaused() || !meter.isRunning()) {
                meter.startTable(context.currentDataCollectionId().getId().identifier());
            }
            meter.remainingTables(context.dataCollectionsToBeSnapshottedCount());
            if (context.isSnapshotPaused()) {
                meter.paused();
            }
        }
        try {
            super.readChunk(partition, offset);
        }
        catch (InterruptedException | RuntimeException e) {
            meter.failed();
            throw e;
        }
    }

    @Override
    protected void sendWindowEvents(DamengPartition partition, OffsetContext offset) throws InterruptedException {
        // The superclass invokes this only after accepting the current CLOSE signal.
        final boolean measured = meter.windowCloseReceived(context.currentChunkId() + "-close");
        final long started = clock.currentTimeInNanos();
        try {
            super.sendWindowEvents(partition, offset);
            if (measured) {
                meter.windowCompleted(clock.currentTimeInNanos() - started);
            }
        }
        catch (InterruptedException | RuntimeException e) {
            meter.failed();
            throw e;
        }
    }

    @Override
    public void pauseSnapshot(DamengPartition partition, OffsetContext offset) {
        super.pauseSnapshot(partition, offset);
        if (context.isSnapshotPaused()) {
            meter.paused();
        }
    }

    @Override
    public void resumeSnapshot(DamengPartition partition, OffsetContext offset) throws InterruptedException {
        meter.resumed();
        super.resumeSnapshot(partition, offset);
    }

    @Override
    protected void postIncrementalSnapshotCompleted() {
        meter.finished();
    }

    @Override
    protected void emitWindowOpen(DamengPartition partition, OffsetContext offset) throws SQLException {
        meter.windowOpened(context.currentChunkId());
        if (standby == null) {
            super.emitWindowOpen(partition, offset);
            return;
        }
        standby.validate(connection);
        try {
            writeStandbyWindowOpen(partition);
        }
        catch (SQLException e) {
            // A demotion between validation and INSERT can surface as -710
            // (standby write denied). Recheck roles before classifying the error.
            standby.validate(connection);
            throw e;
        }
        // Without this barrier a lagging replica could resurrect rows deleted
        // before the low watermark, outside the deduplication window.
        standby.awaitReplay(connection, standby.primary().currentLsn());
        connection.commit();
    }

    @Override
    protected void emitWindowClose(DamengPartition partition, OffsetContext offset) throws Exception {
        if (standby != null) {
            standby.validate(connection);
            connection.commit();
        }
        try {
            if (standby == null) {
                super.emitWindowClose(partition, offset);
            }
            else {
                writeStandbyWindowClose(partition, offset);
            }
            if (context.snapshotRunning()) {
                // The superclass may have skipped a failed table before reading this window.
                meter.startTable(context.currentDataCollectionId().getId().identifier());
                meter.remainingTables(context.dataCollectionsToBeSnapshottedCount());
                meter.windowOpened(context.currentChunkId());
                meter.windowCloseWritten();
            }
        }
        catch (SQLException e) {
            if (standby != null) {
                standby.validate(connection);
            }
            throw e;
        }
    }

    private void writeStandbyWindowOpen(DamengPartition partition) throws SQLException {
        final String signalTableName = getSignalTableNameForPartition(partition);
        if (signalTableName == null) {
            LOGGER.warn("Not able to determine signal table, cannot emit window open signal");
            return;
        }

        final var primary = standby.primary();
        final String statement = "INSERT INTO " + signalTableName + " VALUES (?, ?, ?)";
        final var metadata = new SignalMetadata(Instant.now(), null);
        standbySignalMetadata = metadata;
        primary.prepareUpdate(statement, ps -> {
            LOGGER.trace("Emitting open window for chunk = '{}' to signal table '{}'", context.currentChunkId(), signalTableName);
            ps.setString(1, context.currentChunkId() + "-open");
            ps.setString(2, OpenIncrementalSnapshotWindow.NAME);
            ps.setString(3, metadata.metadataString());
        });
        primary.commit();
    }

    private void writeStandbyWindowClose(DamengPartition partition, OffsetContext offset) throws Exception {
        final String signalTableName = getSignalTableNameForPartition(partition);
        if (signalTableName == null) {
            LOGGER.warn("Not able to detect signal table, cannot emit window close signal");
            return;
        }

        LOGGER.trace("Emitting close window for chunk = '{}' to signal table '{}'", context.currentChunkId(), signalTableName);
        final var primary = standby.primary();
        final WatermarkWindowCloser closer;
        if (Objects.requireNonNull(config.getIncrementalSnapshotWatermarkingStrategy()) == INSERT_DELETE) {
            closer = new DeleteWindowCloser<>(primary, signalTableName, this);
        }
        else {
            closer = new InsertWindowCloser(primary, signalTableName, new SignalMetadata(standbySignalMetadata.getOpenWindowTimestamp(), Instant.now()));
        }
        closer.closeWindow(partition, offset, context.currentChunkId());
    }

    public DamengIncrementalSnapshotChangeEventSource(DamengConnectorConfig config, DamengConnection connection,
                                                      EventDispatcher<DamengPartition, TableId> dispatcher, DamengDatabaseSchema schema,
                                                      Clock clock, SnapshotProgressListener<DamengPartition> progressListener,
                                                      DataChangeEventListener<DamengPartition> dataChangeEventListener,
                                                      NotificationService<DamengPartition, DamengOffsetContext> notifications) {
        this(config, connection, dispatcher, schema, clock, progressListener, dataChangeEventListener, notifications,
                progressListener instanceof DamengSnapshotChangeEventSourceMetrics metrics ? metrics.incrementalMeter()
                        : new DamengIncrementalSnapshotMeter(clock));
    }

    private DamengIncrementalSnapshotChangeEventSource(DamengConnectorConfig config, DamengConnection connection,
                                                       EventDispatcher<DamengPartition, TableId> dispatcher, DamengDatabaseSchema schema,
                                                       Clock clock, SnapshotProgressListener<DamengPartition> progressListener,
                                                       DataChangeEventListener<DamengPartition> dataChangeEventListener,
                                                       NotificationService<DamengPartition, DamengOffsetContext> notifications,
                                                       DamengIncrementalSnapshotMeter meter) {
        super(config, connection, dispatcher, schema, clock, new DamengIncrementalSnapshotProgressListener(progressListener, meter), dataChangeEventListener,
                notifications);
        this.config = config;
        this.connection = connection;
        this.clock = clock;
        this.meter = meter;
    }

    @Override
    protected SnapshotChangeRecordEmitter<DamengPartition> getChangeRecordEmitter(DamengPartition partition, TableId tableId,
                                                                                  OffsetContext offset, Object[] row) {
        return new SnapshotChangeRecordEmitter<>(partition, offset, row, clock, connectorConfig) {
            @Override
            public void emitChangeRecords(DataCollectionSchema schema, Receiver<DamengPartition> receiver) throws InterruptedException {
                super.emitChangeRecords(schema, (eventPartition, eventSchema, operation, key, value, eventOffset, headers) -> {
                    receiver.changeRecord(eventPartition, eventSchema, operation, key, value, eventOffset, headers);
                    // Count only after the incremental receiver has successfully enqueued the event.
                    meter.eventEnqueued();
                });
            }
        };
    }

    @Override
    protected String getSignalTableName(String dataCollectionId) {
        return connection.quotedTableIdString(TableId.parse(dataCollectionId, false));
    }

    @Override
    protected Optional<String> getDatabaseName(Object dataCollectionId) {
        return Optional.empty();
    }

    @Override
    protected Table readSchemaForTable(TableId tableId) throws SQLException {
        final Table table = super.readSchemaForTable(tableId);
        return table.edit().addAttributes(table.attributes())
                .addAttribute(Attribute.editor().name("dameng.object.id").value(Long.toString(connection.getTableObjectId(tableId))).create())
                .create();
    }
}
