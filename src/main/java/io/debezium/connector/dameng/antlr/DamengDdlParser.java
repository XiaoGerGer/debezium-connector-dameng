/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng.antlr;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.misc.Interval;
import org.antlr.v4.runtime.tree.ParseTree;

import io.debezium.antlr.AntlrDdlParser;
import io.debezium.antlr.AntlrDdlParserListener;
import io.debezium.antlr.DataTypeResolver;
import io.debezium.connector.dameng.antlr.DamengDdlStatementAnalyzer.Analysis;
import io.debezium.connector.dameng.antlr.DamengDdlStatementAnalyzer.Impact;
import io.debezium.connector.dameng.antlr.listener.ColumnDefinitionParser;
import io.debezium.connector.dameng.antlr.listener.DamengDdlParserListener;
import io.debezium.connector.dameng.compatibility.DamengIdentifierRules;
import io.debezium.connector.dameng.compatibility.DamengTypeRules;
import io.debezium.ddl.parser.dameng.generated.DamengSqlLexer;
import io.debezium.ddl.parser.dameng.generated.DamengSqlParser;
import io.debezium.ddl.parser.dameng.generated.DamengSqlParserBaseListener;
import io.debezium.relational.SystemVariables;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.relational.ddl.DdlChanges;
import io.debezium.text.ParsingException;

/** Coordinates complete-input analysis and atomic historical schema changes. */
public final class DamengDdlParser extends AntlrDdlParser<DamengSqlLexer, DamengSqlParser> {
    public static final String CONSTRAINT_PREFIX = ColumnDefinitionParser.CONSTRAINT_PREFIX;

    private final DamengDdlStatementAnalyzer analyzer;
    private final ColumnDefinitionParser columns;
    private final DamengIdentifierRules identifierRules;
    private final boolean nestedComments;
    private Session session;

    public DamengDdlParser() {
        this(StandardCharsets.UTF_8, true, false, false);
    }

    public DamengDdlParser(Charset databaseCharset, boolean characterSemanticsEnabled, boolean defaultCharacterLength, boolean nationalCharacterLength) {
        this(databaseCharset, characterSemanticsEnabled, defaultCharacterLength, nationalCharacterLength, false);
    }

    public DamengDdlParser(Charset databaseCharset, boolean characterSemanticsEnabled, boolean defaultCharacterLength, boolean nationalCharacterLength,
                           boolean nestedComments) {
        this(databaseCharset, characterSemanticsEnabled, defaultCharacterLength, nationalCharacterLength, nestedComments, new DamengIdentifierRules(false));
    }

    public DamengDdlParser(Charset databaseCharset, boolean characterSemanticsEnabled, boolean defaultCharacterLength, boolean nationalCharacterLength,
                           boolean nestedComments, DamengIdentifierRules identifierRules) {
        this(databaseCharset, characterSemanticsEnabled, defaultCharacterLength, nationalCharacterLength, nestedComments, identifierRules, DamengTypeRules.NATIVE);
    }

    public DamengDdlParser(Charset databaseCharset, boolean characterSemanticsEnabled, boolean defaultCharacterLength, boolean nationalCharacterLength,
                           boolean nestedComments, DamengIdentifierRules identifierRules, DamengTypeRules typeRules) {
        this(databaseCharset, characterSemanticsEnabled, defaultCharacterLength, nationalCharacterLength, nestedComments, identifierRules, typeRules, false);
    }

    public DamengDdlParser(Charset databaseCharset, boolean characterSemanticsEnabled, boolean defaultCharacterLength, boolean nationalCharacterLength,
                           boolean nestedComments, DamengIdentifierRules identifierRules, DamengTypeRules typeRules, boolean backslashEscape) {
        super(true, false, false);
        this.nestedComments = nestedComments;
        this.identifierRules = identifierRules;
        this.analyzer = new DamengDdlStatementAnalyzer(nestedComments, identifierRules);
        this.columns = new ColumnDefinitionParser(databaseCharset, characterSemanticsEnabled, defaultCharacterLength,
                nationalCharacterLength, identifierRules, typeRules, backslashEscape);
    }

    public enum Type {
        CREATE,
        ALTER,
        DROP,
        TRUNCATE,
        RENAME
    }

    public record SchemaChange(Type type, Table table, TableId tableId, TableId previousTableId) {
    }

    /** Analyze one complete LogMiner record before filtering or changing its historical model. */
    public Analysis analyze(String ddl, String defaultSchema) {
        return analyzer.analyze(ddl, defaultSchema).requireSupported();
    }

    public TableId resolveTableId(String ddl, String defaultSchema) {
        return analyze(ddl, defaultSchema).tableId();
    }

    public TableId resolveRenamedTableId(String ddl, String defaultSchema) {
        return analyze(ddl, defaultSchema).renamedTableId();
    }

    public boolean isNonTableDdl(String ddl) {
        return analyzer.analyze(ddl, null).impact() == Impact.NO_TABLE_CHANGE;
    }

    /** Computes a single historical change without modifying the live model. */
    public List<SchemaChange> plan(Analysis analysis, Tables tables) {
        analysis.requireSupported();
        if (analysis.impact() == Impact.NO_TABLE_CHANGE) {
            return List.of();
        }
        setCurrentSchema(analysis.defaultSchema());
        try {
            return parseModel(analysis.sql(), tables, analysis.tokens(), false).changes();
        }
        catch (ParsingException failure) {
            throw legacyFailure(failure);
        }
    }

    /** Public Debezium parser contract; publish only after all statements have succeeded. */
    @Override
    public DdlChanges parse(String ddl, Tables tables) {
        return parseModel(ddl, tables, null, true).ddlChanges();
    }

    public List<SchemaChange> parse(String ddl, Tables tables, String defaultSchema) {
        setCurrentSchema(defaultSchema);
        try {
            return parseModel(ddl, tables, null, true).changes();
        }
        catch (ParsingException failure) {
            throw legacyFailure(failure);
        }
    }

    private ParseResult parseModel(String ddl, Tables tables, List<DamengSqlToken> selectedTokens, boolean apply) {
        Objects.requireNonNull(ddl, "ddl");
        Objects.requireNonNull(tables, "tables");
        session = new Session(ddl, selectedTokens);
        try {
            final var ddlChanges = super.parse(ddl, tables.clone());
            final var changes = List.copyOf(session.changes);
            if (apply) {
                applyChanges(tables, changes);
            }
            return new ParseResult(ddlChanges, changes);
        }
        catch (IllegalArgumentException failure) {
            // Keep the original domain exception available to the streaming policy boundary.
            final var position = failure instanceof DamengSqlToken.ParsingFailure located ? located.position() : null;
            throw new ParsingException(position, failure.getMessage(), failure);
        }
        finally {
            // The base parser resets events only on success. A failed script must not leak them.
            getAndResetDdlChanges();
            databaseTables = null;
            session = null;
        }
    }

    private static void applyChanges(Tables tables, List<SchemaChange> changes) {
        for (var change : changes) {
            if (change.previousTableId() != null) {
                tables.removeTable(change.previousTableId());
            }
            if (change.type() == Type.DROP) {
                tables.removeTable(change.tableId());
            }
            else {
                tables.overwriteTable(change.table());
            }
        }
    }

    private static IllegalArgumentException legacyFailure(ParsingException failure) {
        return failure.getCause() instanceof IllegalArgumentException original
                ? original
                : new IllegalArgumentException(failure.getMessage(), failure);
    }

    @Override
    protected DamengSqlLexer createNewLexerInstance(CharStream input) {
        // Lexer actions inspect case-sensitive quote delimiters. Recover original characters
        // from the parent's CaseChangingCharStream; the grammar handles keyword case itself.
        return DamengAntlrSqlLexer.lexer(input.getText(Interval.of(0, input.size() - 1)), nestedComments);
    }

    @Override
    protected DamengSqlParser createNewParserInstance(CommonTokenStream tokens) {
        if (session.selectedTokens == null) {
            tokens.fill();
            session.lexed = DamengAntlrSqlLexer.adapt(session.sql, tokens.getTokens());
        }
        else {
            // Analysis may describe a selected interval of a larger source script.
            session.lexed = DamengParseTree.selectedTokens(session.sql, session.selectedTokens);
        }
        return DamengParseTree.parser(session.sql, session.lexed);
    }

    @Override
    protected ParseTree parseTree(DamengSqlParser parser) {
        final var tree = parser.ddlScript();
        final var parsed = new DamengParseTree.Parsed<>(session.sql, session.lexed.tokens(), tree);
        for (var analysis : analyzer.analyzeScript(parsed, currentSchema())) {
            if (analysis.impact() == Impact.TABLE) {
                session.statements.put(analysis.tableStatement(), analysis);
            }
        }
        return tree;
    }

    @Override
    protected AntlrDdlParserListener createParseTreeWalkerListener() {
        return new ScriptListener();
    }

    @Override
    protected String getStatementText(ParserRuleContext context) {
        // Adapted token offsets refer to Java UTF-16 positions in the original source.
        return DamengParseTree.source(context);
    }

    @Override
    protected boolean isGrammarInUpperCase() {
        return true;
    }

    @Override
    public DataTypeResolver dataTypeResolver() {
        throw new UnsupportedOperationException("Dameng type resolution requires ColumnDefinitionParser and compatibility settings");
    }

    @Override
    protected SystemVariables createNewSystemVariablesInstance() {
        return new SystemVariables();
    }

    @Override
    public void setCurrentDatabase(String databaseName) {
        // Dameng table identifiers use schema and table; the database is not a schema alias.
    }

    private record ParseResult(DdlChanges ddlChanges, List<SchemaChange> changes) {
    }

    private static final class Session {
        private final String sql;
        private final List<DamengSqlToken> selectedTokens;
        private final List<SchemaChange> changes = new ArrayList<>();
        private final Map<DamengSqlParser.TableStatementContext, Analysis> statements = new HashMap<>();
        private DamengAntlrSqlLexer.Lexed lexed;
        private DamengDdlParserListener delegate;

        private Session(String sql, List<DamengSqlToken> selectedTokens) {
            this.sql = sql;
            this.selectedTokens = selectedTokens;
        }
    }

    private final class ScriptListener extends DamengSqlParserBaseListener implements AntlrDdlParserListener {
        @Override
        public Collection<ParsingException> getErrors() {
            // Domain listeners fail immediately, before any staged model is published.
            return List.of();
        }

        @Override
        public void enterTableStatement(DamengSqlParser.TableStatementContext context) {
            final var analysis = session.statements.get(context);
            if (analysis != null) {
                session.delegate = new DamengDdlParserListener(analysis, databaseTables, columns, identifierRules);
                session.delegate.enterEveryRule(context);
                session.delegate.enterTableStatement(context);
            }
        }

        @Override
        public void exitTableStatement(DamengSqlParser.TableStatementContext context) {
            if (session.delegate == null) {
                return;
            }
            session.delegate.exitEveryRule(context);
            final var change = session.delegate.change();
            session.changes.add(change);
            switch (change.type()) {
                case CREATE -> signalCreateTable(change.tableId(), context);
                case ALTER, RENAME -> signalAlterTable(change.tableId(), change.previousTableId(), context);
                case DROP -> signalDropTable(change.tableId(), context);
                case TRUNCATE -> signalTruncateTable(change.tableId(), context);
            }
            session.delegate = null;
        }

        @Override
        public void enterEveryRule(ParserRuleContext context) {
            if (session.delegate != null) {
                session.delegate.enterEveryRule(context);
            }
        }

        @Override
        public void exitEveryRule(ParserRuleContext context) {
            if (session.delegate != null) {
                session.delegate.exitEveryRule(context);
            }
        }
    }
}
