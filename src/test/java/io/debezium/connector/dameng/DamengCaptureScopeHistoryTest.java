/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.sql.Types;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.relational.Attribute;
import io.debezium.relational.Column;
import io.debezium.relational.CustomConverterRegistry;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.schema.SchemaChangeEvent;
import io.debezium.schema.SchemaChangeEvent.SchemaChangeEventType;
import io.debezium.storage.file.history.FileSchemaHistory;

class DamengCaptureScopeHistoryTest {
    @TempDir
    Path directory;

    @Test
    void shouldRecoverAnAddedTablesHistoricalStructureWithoutMovingTheCheckpoint() throws Exception {
        final var original = config("APP[.]A");
        final var expanded = config("APP[.](A|B)");
        final var partition = new DamengPartition(original.getLogicalName());
        final var checkpoint = new DamengOffsetContext(original, 10);
        final var a = table("A", 101);
        final var b = table("B", 102);
        final var altered = b.edit().addAttributes(b.attributes())
                .addColumn(Column.editor().name("LABEL").position(2).jdbcType(Types.VARCHAR).type("VARCHAR").create()).create();
        try (var writer = schema(original)) {
            writer.initializeStorage();
            writer.applySchemaChange(SchemaChangeEvent.ofSnapshotCreate(partition, checkpoint, "DAMENG", a));
            writer.applySchemaChange(SchemaChangeEvent.ofSnapshotCreate(partition, checkpoint, "DAMENG", b));
            assertThat(writer.getTables().forTable(b.id())).isEqualTo(b);
            assertThat(writer.schemaFor(b.id())).isNull();
            final var ddlOffset = new DamengOffsetContext(original, 10);
            ddlOffset.setCommitLsn(20);
            writer.applySchemaChange(SchemaChangeEvent.of(SchemaChangeEventType.ALTER, partition, ddlOffset,
                    "DAMENG", "APP", "ALTER TABLE APP.B ADD LABEL VARCHAR(100)", altered, false));
        }

        final var resumed = new DamengOffsetContext.Loader(expanded).load(checkpoint.getOffset());
        try (var reader = schema(expanded)) {
            reader.recover(Offsets.of(partition, resumed));
            assertThat(reader.getTables().forTable(b.id())).isEqualTo(b);
            assertThat(reader.schemaFor(b.id())).isNotNull();
            assertThat(resumed.getLsn()).isEqualTo(10);
            assertThat(resumed.getCommitLsn()).isEqualTo(10);
            resumed.setCommitLsn(20);
            reader.recover(Offsets.of(partition, resumed));
            assertThat(reader.getTables().forTable(b.id())).isEqualTo(altered);
            assertThat(resumed.getLsn()).isEqualTo(10);
        }
    }

    private DamengConnectorConfig config(String included) {
        return new DamengConnectorConfig(DamengTestConfiguration.configuration()
                .with("table.include.list", included)
                .with("schema.history.internal", FileSchemaHistory.class.getName())
                .with("schema.history.internal.file.filename", directory.resolve("history.dat").toString()).build());
    }

    private static DamengDatabaseSchema schema(DamengConnectorConfig config) {
        return new DamengDatabaseSchema(config, new DamengValueConverters(config), config.schemaNameAdjuster(),
                config.getTopicNamingStrategy(CommonConnectorConfig.TOPIC_NAMING_STRATEGY), new CustomConverterRegistry(List.of()),
                new DamengTaskContext(config.getConfig(), config));
    }

    private static Table table(String name, long objectId) {
        return Table.editor().tableId(new TableId(null, "APP", name))
                .addAttribute(Attribute.editor().name("dameng.object.id").value(Long.toString(objectId)).create())
                .addColumn(Column.editor().name("ID").position(1).jdbcType(Types.INTEGER).type("INTEGER").optional(false).create())
                .setPrimaryKeyNames("ID").create();
    }
}
