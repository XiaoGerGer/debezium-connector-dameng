/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class DamengCaptureFilterChangeTest {
    @ParameterizedTest
    @CsvSource({ "APP[.]A,APP[.](A|B)", "APP[.](A|B),APP[.]A", "APP[.]A,APP\\.A" })
    void shouldKeepTheReplayAndTransactionPositionAfterChangingTableFilters(String before, String after) {
        final var configuration = DamengTestConfiguration.configuration().build();
        final var original = new DamengConnectorConfig(configuration.edit().with("table.include.list", before).build());
        final var expanded = new DamengConnectorConfig(configuration.edit().with("table.include.list", after).build());
        final var checkpoint = new DamengOffsetContext(original, 40);
        checkpoint.setChangeLsn(65);
        checkpoint.setCommitLsn(70);
        checkpoint.setTransactionId("01");
        checkpoint.setEventSerialNo(3);
        checkpoint.validateDatabaseMagic(42);

        final var resumed = new DamengOffsetContext.Loader(expanded).load(checkpoint.getOffset());

        assertThat(resumed.getLsn()).isEqualTo(40);
        assertThat(resumed.getChangeLsn()).isEqualTo(65);
        assertThat(resumed.getCommitLsn()).isEqualTo(70);
        assertThat(resumed.getTransactionId()).isEqualTo("01");
        assertThat(resumed.getEventSerialNo()).isEqualTo(3);
        assertThat(resumed.getDatabaseMagic()).isEqualTo(42);
    }
}
