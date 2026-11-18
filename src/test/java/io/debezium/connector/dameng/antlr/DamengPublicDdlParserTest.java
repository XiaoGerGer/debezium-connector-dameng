/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng.antlr;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.debezium.relational.Tables;
import io.debezium.relational.ddl.DdlChanges;
import io.debezium.relational.ddl.DdlParser;
import io.debezium.relational.ddl.DdlParserListener.Event;
import io.debezium.relational.ddl.DdlParserListener.EventType;
import io.debezium.text.ParsingException;

class DamengPublicDdlParserTest {
    private DdlParser parser() {
        return new DamengDdlParser();
    }

    @Test
    void shouldApplyAndReportOrderedChangesThroughThePublicInterface() {
        final var parser = parser();
        final var tables = new Tables();
        parser.setCurrentDatabase("DM");
        parser.setCurrentSchema("APP");

        final var changes = parser.parse("CREATE TABLE T(ID INT); ALTER TABLE T ADD V INT;"
                + " ALTER TABLE T RENAME TO U; TRUNCATE TABLE U; DROP TABLE U", tables);

        assertThat(events(changes)).extracting(Event::type).containsExactly(EventType.CREATE_TABLE,
                EventType.ALTER_TABLE, EventType.ALTER_TABLE, EventType.TRUNCATE_TABLE, EventType.DROP_TABLE);
        assertThat(tables.tableIds()).isEmpty();
    }

    @Test
    void shouldDiscardPartialModelsAndEventsBeforeReusingTheParser() {
        final var parser = parser();
        final var tables = new Tables();
        parser.setCurrentSchema("APP");
        parser.parse("CREATE TABLE T(ID INT)", tables);
        final var original = tables.forTable(null, "APP", "T");

        assertThatThrownBy(() -> parser.parse("ALTER TABLE T ADD V INT; ALTER TABLE T MODIFY MISSING INT", tables))
                .isInstanceOf(ParsingException.class);
        assertThat(tables.forTable(null, "APP", "T")).isSameAs(original);

        final var changes = parser.parse("ALTER TABLE T ADD W INT", tables);
        assertThat(events(changes)).singleElement().satisfies(event -> assertThat(event.statement()).isEqualTo("ALTER TABLE T ADD W INT"));
        assertThat(tables.forTable(null, "APP", "T").retrieveColumnNames()).containsExactly("ID", "W");
    }

    @Test
    void shouldPreserveCaseSensitiveQuotedDefaultsAndOriginalStatementText() {
        final var parser = parser();
        final var tables = new Tables();
        parser.setCurrentSchema("APP");
        final var sql = "create table \"MiXeD\"(ID INT, V VARCHAR(40) DEFAULT q'a😀A'a')";

        final var changes = parser.parse("/*😀*/ " + sql, tables);

        assertThat(events(changes)).singleElement().satisfies(event -> assertThat(event.statement()).isEqualTo(sql));
        assertThat(tables.forTable(null, "APP", "MiXeD").columnWithName("V").defaultValueExpression()).contains("q'a😀A'a'");
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "/*😀*/ CREATE TABLE APP.T(ID INT, V VARCHAR(4) DEFAULT 'unfinished)",
            "/*😀*/ CREATE TABLE APP.T(ID INT) UNRECOGNIZED",
            "/*😀*/ CREATE TABLE APP.T(ID UNKNOWN_TYPE)"
    })
    void shouldExposeErrorPositionsAndRecoverAfterLexicalSyntaxOrSemanticFailure(String sql) {
        final var parser = parser();
        final var tables = new Tables();

        assertThatThrownBy(() -> parser.parse(sql, tables)).isInstanceOfSatisfying(ParsingException.class, failure -> {
            assertThat(failure.getPosition()).isNotNull();
            assertThat(failure.getPosition().index()).isGreaterThanOrEqualTo(8);
            assertThat(failure.getPosition().line()).isEqualTo(1);
            assertThat(failure.getPosition().column()).isEqualTo(failure.getPosition().index() + 1);
        });
        assertThat(tables.tableIds()).isEmpty();
        assertThat(events(parser.parse("CREATE TABLE APP.OK(ID INT)", tables))).hasSize(1);
        assertThat(tables.forTable(null, "APP", "OK").retrieveColumnNames()).containsExactly("ID");
    }

    @Test
    void shouldPlanOnlySelectedTokensWithoutApplyingThem() {
        final var parser = new DamengDdlParser();
        final var tables = new Tables();
        parser.parse("CREATE TABLE APP.T(ID INT)", tables, "APP");
        final var original = tables.forTable(null, "APP", "T");
        final var selectedSql = "ALTER TABLE T ADD V INT";
        final var sql = "/*😀*/ UNSUPPORTED BEFORE; " + selectedSql + "; UNSUPPORTED AFTER";
        final int start = sql.indexOf("ALTER TABLE");
        final var selected = new DamengAntlrSqlLexer().tokenize(sql).stream()
                .filter(token -> token.start() >= start && token.end() <= start + selectedSql.length()).toList();
        final var analysis = new DamengDdlStatementAnalyzer().analyzeTokens(sql, selected, "APP").requireSupported();

        final var changes = parser.plan(analysis, tables);

        assertThat(changes).singleElement().satisfies(change -> assertThat(change.table().retrieveColumnNames()).containsExactly("ID", "V"));
        assertThat(tables.forTable(null, "APP", "T")).isSameAs(original);
        assertThat(original.retrieveColumnNames()).containsExactly("ID");
    }

    private List<Event> events(DdlChanges changes) {
        final List<Event> events = new ArrayList<>();
        changes.getEventsByDatabase((database, batch) -> events.addAll(batch));
        return events;
    }
}
