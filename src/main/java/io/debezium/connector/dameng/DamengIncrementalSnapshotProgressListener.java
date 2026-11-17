/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.relational.TableId;
import io.debezium.spi.schema.DataCollectionId;

/** Observes incremental progress without including rows from the initial snapshot. */
final class DamengIncrementalSnapshotProgressListener implements SnapshotProgressListener<DamengPartition> {
    private final SnapshotProgressListener<DamengPartition> delegate;
    private final DamengIncrementalSnapshotMeter meter;
    private long previousRowsScanned;

    DamengIncrementalSnapshotProgressListener(SnapshotProgressListener<DamengPartition> delegate, DamengIncrementalSnapshotMeter meter) {
        this.delegate = delegate;
        this.meter = meter;
    }

    @Override
    public void snapshotStarted(DamengPartition partition) {
        delegate.snapshotStarted(partition);
    }

    @Override
    public void snapshotPaused(DamengPartition partition) {
        delegate.snapshotPaused(partition);
    }

    @Override
    public void snapshotResumed(DamengPartition partition) {
        delegate.snapshotResumed(partition);
    }

    @Override
    public void monitoredDataCollectionsDetermined(DamengPartition partition, Iterable<? extends DataCollectionId> dataCollectionIds) {
        delegate.monitoredDataCollectionsDetermined(partition, dataCollectionIds);
    }

    @Override
    public void snapshotCompleted(DamengPartition partition) {
        delegate.snapshotCompleted(partition);
    }

    @Override
    public void snapshotAborted(DamengPartition partition) {
        delegate.snapshotAborted(partition);
    }

    @Override
    public void snapshotSkipped(DamengPartition partition) {
        delegate.snapshotSkipped(partition);
    }

    @Override
    public void dataCollectionSnapshotCompleted(DamengPartition partition, DataCollectionId dataCollectionId, long numRows) {
        delegate.dataCollectionSnapshotCompleted(partition, dataCollectionId, numRows);
    }

    @Override
    public void rowsScanned(DamengPartition partition, TableId tableId, long numRows) {
        // The framework reports a cumulative count, resetting it when it clears the current chunk.
        // A JMX counter reset must not reset this baseline and recount earlier chunks.
        final long rows = numRows >= previousRowsScanned ? numRows - previousRowsScanned : numRows;
        previousRowsScanned = numRows;
        meter.rowsScanned(tableId.identifier(), rows);
        delegate.rowsScanned(partition, tableId, numRows);
    }

    @Override
    public void currentChunk(DamengPartition partition, String chunkId, Object[] chunkFrom, Object[] chunkTo) {
        if (chunkId == null) {
            previousRowsScanned = 0;
        }
        delegate.currentChunk(partition, chunkId, chunkFrom, chunkTo);
    }

    @Override
    public void currentChunk(DamengPartition partition, String chunkId, Object[] chunkFrom, Object[] chunkTo, Object[] tableTo) {
        if (chunkId == null) {
            previousRowsScanned = 0;
        }
        delegate.currentChunk(partition, chunkId, chunkFrom, chunkTo, tableTo);
    }

    @Override
    public void chunkProgress(DamengPartition partition, TableId tableId, long totalChunks, long completedChunks) {
        delegate.chunkProgress(partition, tableId, totalChunks, completedChunks);
    }
}
