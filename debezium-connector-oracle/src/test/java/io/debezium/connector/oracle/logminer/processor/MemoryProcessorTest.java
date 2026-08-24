/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.processor;

import static org.fest.assertions.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleConnectorConfig.LogMiningBufferType;
import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.junit.SkipWhenAdapterNameIsNot;
import io.debezium.connector.oracle.logminer.events.LogMinerEventRow;
import io.debezium.connector.oracle.logminer.processor.memory.MemoryLogMinerEventProcessor;
import io.debezium.connector.oracle.util.TestHelper;

/**
 * @author Chris Cranford
 */
@SkipWhenAdapterNameIsNot(value = SkipWhenAdapterNameIsNot.AdapterName.LOGMINER, reason = "Only applicable for LogMiner")
public class MemoryProcessorTest extends AbstractProcessorUnitTest<MemoryLogMinerEventProcessor> {

    private static final Logger LOGGER = LoggerFactory.getLogger(MemoryProcessorTest.class);

    @Override
    protected Configuration.Builder getConfig() {
        return TestHelper.defaultConfig()
                .with(OracleConnectorConfig.LOG_MINING_BUFFER_TYPE, LogMiningBufferType.MEMORY)
                .with(OracleConnectorConfig.LOG_MINING_BUFFER_DROP_ON_STOP, true);
    }

    @Override
    protected MemoryLogMinerEventProcessor getProcessor(OracleConnectorConfig connectorConfig) {
        assertThat(connectorConfig.validateAndRecord(OracleConnectorConfig.ALL_FIELDS, LOGGER::error)).isTrue();
        return new MemoryLogMinerEventProcessor(context,
                connectorConfig,
                connection,
                dispatcher,
                partition,
                offsetContext,
                schema,
                metrics);
    }

    @Test
    public void shouldKeepMissingTransactionUntilMinedThroughObservedCurrentScn() throws Exception {
        final OracleConnectorConfig config = new OracleConnectorConfig(getConfig().build());
        connection = createConnectionForTransactionActiveCheck(false, Scn.valueOf(200L));
        try (TestableMemoryLogMinerEventProcessor processor = getTestableProcessor(config)) {
            processor.handleStart(getStartLogMinerEventRow(Scn.valueOf(100L), "17001f005ac90000"));
            processor.handleDataEvent(getInsertLogMinerEventRow(Scn.valueOf(101L), "17001f005ac90000"));

            processor.abandonTransactionsAfterEmptyPlSqlOutputWindowForTest(Scn.valueOf(150L));

            assertThat(processor.getTransactionCache().get("17001f005ac90000")).isNotNull();
            assertThat(metrics.getAbandonedTransactionIds()).isEmpty();
        }
    }

    @Test
    public void shouldAbandonMissingTransactionAfterMinedThroughObservedCurrentScnAndConfirmedMissing() throws Exception {
        final OracleConnectorConfig config = new OracleConnectorConfig(getConfig().build());
        connection = createConnectionForTransactionActiveCheck(false, Scn.valueOf(200L));
        try (TestableMemoryLogMinerEventProcessor processor = getTestableProcessor(config)) {
            processor.handleStart(getStartLogMinerEventRow(Scn.valueOf(100L), "17001f005ac90000"));
            processor.handleDataEvent(getInsertLogMinerEventRow(Scn.valueOf(101L), "17001f005ac90000"));

            processor.abandonTransactionsAfterEmptyPlSqlOutputWindowForTest(Scn.valueOf(150L));
            processor.abandonTransactionsAfterEmptyPlSqlOutputWindowForTest(Scn.valueOf(200L));

            assertThat(processor.getTransactionCache().get("17001f005ac90000")).isNull();
            assertThat(metrics.getAbandonedTransactionIds()).contains("17001f005ac90000");
        }
    }

    @Test
    public void shouldClearMissingObservationWhenTransactionBecomesActiveAgain() throws Exception {
        final OracleConnectorConfig config = new OracleConnectorConfig(getConfig().build());
        connection = createConnectionForTransactionActiveCheck(false, true, false);
        Mockito.when(connection.getCurrentScn()).thenReturn(Scn.valueOf(200L));
        try (TestableMemoryLogMinerEventProcessor processor = getTestableProcessor(config)) {
            processor.handleStart(getStartLogMinerEventRow(Scn.valueOf(100L), "17001f005ac90000"));
            processor.handleDataEvent(getInsertLogMinerEventRow(Scn.valueOf(101L), "17001f005ac90000"));

            processor.abandonTransactionsAfterEmptyPlSqlOutputWindowForTest(Scn.valueOf(150L));
            processor.abandonTransactionsAfterEmptyPlSqlOutputWindowForTest(Scn.valueOf(200L));
            processor.abandonTransactionsAfterEmptyPlSqlOutputWindowForTest(Scn.valueOf(200L));

            assertThat(processor.getTransactionCache().get("17001f005ac90000")).isNotNull();
            assertThat(metrics.getAbandonedTransactionIds()).isEmpty();
        }
    }

    private TestableMemoryLogMinerEventProcessor getTestableProcessor(OracleConnectorConfig connectorConfig) {
        assertThat(connectorConfig.validateAndRecord(OracleConnectorConfig.ALL_FIELDS, LOGGER::error)).isTrue();
        return new TestableMemoryLogMinerEventProcessor(context,
                connectorConfig,
                connection,
                dispatcher,
                partition,
                offsetContext,
                schema,
                metrics);
    }

    private OracleConnection createConnectionForTransactionActiveCheck(boolean active, Scn currentScn) throws Exception {
        final OracleConnection connection = createConnectionForTransactionActiveCheck(active);
        Mockito.when(connection.getCurrentScn()).thenReturn(currentScn);
        return connection;
    }

    private OracleConnection createConnectionForTransactionActiveCheck(boolean... activeResults) throws Exception {
        final OracleConnection connection = Mockito.mock(OracleConnection.class);
        final Connection jdbc = Mockito.mock(Connection.class);
        Mockito.when(connection.connection()).thenReturn(jdbc);
        Mockito.when(connection.singleOptionalValue(anyString(), Mockito.any())).thenReturn(2.f);

        final List<PreparedStatement> statements = new ArrayList<>();
        for (boolean active : activeResults) {
            final PreparedStatement statement = Mockito.mock(PreparedStatement.class);
            final ResultSet resultSet = Mockito.mock(ResultSet.class);
            Mockito.when(resultSet.next()).thenReturn(true);
            Mockito.when(resultSet.getInt(1)).thenReturn(active ? 1 : 0);
            Mockito.when(statement.executeQuery()).thenReturn(resultSet);
            statements.add(statement);
        }
        if (!statements.isEmpty()) {
            Mockito.when(jdbc.prepareStatement(anyString()))
                    .thenReturn(statements.get(0), statements.subList(1, statements.size()).toArray(new PreparedStatement[0]));
        }
        return connection;
    }

    private LogMinerEventRow getStartLogMinerEventRow(Scn scn, String transactionId) {
        LogMinerEventRow row = Mockito.mock(LogMinerEventRow.class);
        Mockito.when(row.getEventType()).thenReturn(io.debezium.connector.oracle.logminer.events.EventType.START);
        Mockito.when(row.getTransactionId()).thenReturn(transactionId);
        Mockito.when(row.getScn()).thenReturn(scn);
        Mockito.when(row.getChangeTime()).thenReturn(java.time.Instant.now());
        return row;
    }

    private LogMinerEventRow getInsertLogMinerEventRow(Scn scn, String transactionId) {
        LogMinerEventRow row = Mockito.mock(LogMinerEventRow.class);
        Mockito.when(row.getEventType()).thenReturn(io.debezium.connector.oracle.logminer.events.EventType.INSERT);
        Mockito.when(row.getTransactionId()).thenReturn(transactionId);
        Mockito.when(row.getScn()).thenReturn(scn);
        Mockito.when(row.getChangeTime()).thenReturn(java.time.Instant.now());
        Mockito.when(row.getRowId()).thenReturn("1234567890");
        Mockito.when(row.getOperation()).thenReturn("INSERT");
        Mockito.when(row.getTableName()).thenReturn("TEST_TABLE");
        Mockito.when(row.getTableId()).thenReturn(io.debezium.relational.TableId.parse("ORCLPDB1.DEBEZIUM.TEST_TABLE"));
        Mockito.when(row.getRedoSql()).thenReturn("insert into \"DEBEZIUM\".\"TEST_TABLE\"(\"ID\",\"DATA\") values ('1','Test');");
        Mockito.when(row.getRsId()).thenReturn("A.B.C");
        Mockito.when(row.getTablespaceName()).thenReturn("DEBEZIUM");
        Mockito.when(row.getUserName()).thenReturn(TestHelper.SCHEMA_USER);
        return row;
    }

    private static class TestableMemoryLogMinerEventProcessor extends MemoryLogMinerEventProcessor {

        TestableMemoryLogMinerEventProcessor(io.debezium.pipeline.source.spi.ChangeEventSource.ChangeEventSourceContext context,
                                             OracleConnectorConfig connectorConfig,
                                             OracleConnection jdbcConnection,
                                             io.debezium.pipeline.EventDispatcher<io.debezium.connector.oracle.OraclePartition, io.debezium.relational.TableId> dispatcher,
                                             io.debezium.connector.oracle.OraclePartition partition,
                                             io.debezium.connector.oracle.OracleOffsetContext offsetContext,
                                             io.debezium.connector.oracle.OracleDatabaseSchema schema,
                                             io.debezium.connector.oracle.OracleStreamingChangeEventSourceMetrics metrics) {
            super(context, connectorConfig, jdbcConnection, dispatcher, partition, offsetContext, schema, metrics);
        }

        void abandonTransactionsAfterEmptyPlSqlOutputWindowForTest(Scn endScn) throws InterruptedException {
            super.abandonTransactionsAfterEmptyPlSqlOutputWindow(endScn);
        }
    }
}
