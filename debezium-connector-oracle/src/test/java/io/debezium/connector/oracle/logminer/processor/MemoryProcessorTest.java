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
import java.sql.SQLException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnectorConfig.LogMiningBufferType;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.junit.SkipWhenAdapterNameIsNot;
import io.debezium.connector.oracle.logminer.events.LogMinerEventRow;
import io.debezium.connector.oracle.logminer.processor.memory.MemoryLogMinerEventProcessor;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.junit.logging.LogInterceptor;

import oracle.jdbc.OracleCallableStatement;

/**
 * @author Chris Cranford
 */
@SkipWhenAdapterNameIsNot(value = SkipWhenAdapterNameIsNot.AdapterName.LOGMINER, reason = "Only applicable for LogMiner")
public class MemoryProcessorTest extends AbstractProcessorUnitTest<MemoryLogMinerEventProcessor> {

    private static final Logger LOGGER = LoggerFactory.getLogger(MemoryProcessorTest.class);
    private OracleConnection diagnosticConnection;

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

    @Test
    public void shouldVerifyEmptyOutputBeforeAdvancingAndKeepActiveTransactions() throws Exception {
        LogMinerDictionaryRecoveryTest.Database db = new LogMinerDictionaryRecoveryTest.Database();
        configureOutput(db, new String[]{ "@END|120|complete" });
        try (TestableMemoryLogMinerEventProcessor processor = plSqlProcessor()) {
            processor.handleStart(getStartLogMinerEventRow(Scn.valueOf(90), "abcdef"));
            processor.handleDataEvent(getInsertLogMinerEventRow(Scn.valueOf(91), "abcdef"));
            assertThat(processor.process(partition, Scn.valueOf(100), Scn.valueOf(120))).isEqualTo(Scn.valueOf(120));
            assertThat(processor.getTransactionCache().get("abcdef")).isNotNull();
            Mockito.verify(offsetContext).setScn(Scn.valueOf(90));
            Mockito.verify(offsetContext, Mockito.never()).setScn(Scn.valueOf(120));
            assertThat(metrics.getAbandonedTransactionIds()).isEmpty();
            assertThat(db.countModes).containsExactly("raw");
        }
    }

    @Test
    public void shouldProcessNormalOnlineRowsWithoutRawFallback() throws Exception {
        LogMinerDictionaryRecoveryTest.Database db = new LogMinerDictionaryRecoveryTest.Database();
        configureOutput(db, new String[]{ boundaryLine(110, 36), "", "@END|120|complete" });
        try (TestableMemoryLogMinerEventProcessor processor = plSqlProcessor()) {
            processor.handleStart(getStartLogMinerEventRow(Scn.valueOf(90), "abcdef"));
            processor.handleDataEvent(getInsertLogMinerEventRow(Scn.valueOf(91), "abcdef"));
            assertThat(processor.process(partition, Scn.valueOf(100), Scn.valueOf(120))).isEqualTo(Scn.valueOf(110));
            assertThat(processor.getTransactionCache()).isEmpty();
            assertThat(db.starts).isEmpty();
        }
    }

    @Test
    public void shouldConsumeRawCommitAfterValidatingEntireSysScn() throws Exception {
        LogMinerDictionaryRecoveryTest.Database db = new LogMinerDictionaryRecoveryTest.Database(
                LogMinerDictionaryRecoveryTest.row(101, 1, "9876"), LogMinerDictionaryRecoveryTest.row(101, 7, null));
        configureOutput(db, new String[]{ "@END|120|complete" });
        try (TestableMemoryLogMinerEventProcessor processor = plSqlProcessor()) {
            processor.handleStart(getStartLogMinerEventRow(Scn.valueOf(90), "abcdef"));
            processor.handleDataEvent(getInsertLogMinerEventRow(Scn.valueOf(91), "abcdef"));
            assertThat(processor.process(partition, Scn.valueOf(100), Scn.valueOf(120))).isEqualTo(Scn.valueOf(101));
            assertThat(processor.getTransactionCache()).isEmpty();
            Mockito.verify(offsetContext).setScn(Scn.valueOf(101));
            assertThat(offsetContext.getCommitScn().getMaxCommittedScn()).isEqualTo(Scn.valueOf(101));
        }
    }

    @Test
    public void shouldReplayCommitBeforeBadScnWithoutCrossingBadScn() throws Exception {
        LogMinerDictionaryRecoveryTest.Database db = new LogMinerDictionaryRecoveryTest.Database(
                LogMinerDictionaryRecoveryTest.row(105, 7, null), LogMinerDictionaryRecoveryTest.row(110, 1, "9876"));
        configureOutput(db, new String[]{ "@END|120|complete" },
                new String[]{ boundaryLine(105, 7), "", "@END|109|complete" });
        try (TestableMemoryLogMinerEventProcessor processor = plSqlProcessor()) {
            processor.handleStart(getStartLogMinerEventRow(Scn.valueOf(90), "abcdef"));
            processor.handleDataEvent(getInsertLogMinerEventRow(Scn.valueOf(91), "abcdef"));
            Scn next = processor.process(partition, Scn.valueOf(100), Scn.valueOf(120));
            assertThat(next.compareTo(Scn.valueOf(110)) < 0).isTrue();
            assertThat(processor.getTransactionCache()).isEmpty();
            assertThat(offsetContext.getCommitScn().getMaxCommittedScn()).isEqualTo(Scn.valueOf(105));
        }
    }

    @Test
    public void shouldNotChangeOffsetOrTransactionsWhenFallbackRestoreFails() throws Exception {
        LogMinerDictionaryRecoveryTest.Database db = new LogMinerDictionaryRecoveryTest.Database(
                LogMinerDictionaryRecoveryTest.row(101, 1, "9876"), LogMinerDictionaryRecoveryTest.row(101, 7, null));
        db.failRestore = true;
        configureOutput(db, new String[]{ "@END|120|complete" });
        try (TestableMemoryLogMinerEventProcessor processor = plSqlProcessor()) {
            processor.handleStart(getStartLogMinerEventRow(Scn.valueOf(90), "abcdef"));
            processor.handleDataEvent(getInsertLogMinerEventRow(Scn.valueOf(91), "abcdef"));
            try {
                processor.process(partition, Scn.valueOf(100), Scn.valueOf(120));
                org.junit.Assert.fail("Expected session restore failure");
            }
            catch (SQLException expected) {
                assertThat(expected.getMessage()).contains("restore failed");
            }
            assertThat(processor.getTransactionCache().get("abcdef")).isNotNull();
            Mockito.verify(offsetContext, Mockito.never()).setScn(Mockito.any());
            Mockito.verify(dispatcher, Mockito.never()).dispatchHeartbeatEvent(Mockito.any(), Mockito.any());
        }
    }

    @Test
    public void shouldNotApplyEarlierCommitIfBusinessDmlAppearsLaterAtSameBadScn() throws Exception {
        LogMinerDictionaryRecoveryTest.Database db = new LogMinerDictionaryRecoveryTest.Database(
                LogMinerDictionaryRecoveryTest.row(101, 1, "9876"), LogMinerDictionaryRecoveryTest.row(101, 7, null),
                LogMinerDictionaryRecoveryTest.row(101, 3, "999"));
        configureOutput(db, new String[]{ "@END|120|complete" });
        try (TestableMemoryLogMinerEventProcessor processor = plSqlProcessor()) {
            processor.handleStart(getStartLogMinerEventRow(Scn.valueOf(90), "abcdef"));
            processor.handleDataEvent(getInsertLogMinerEventRow(Scn.valueOf(91), "abcdef"));
            try {
                processor.process(partition, Scn.valueOf(100), Scn.valueOf(120));
                org.junit.Assert.fail("Expected unsafe metadata rejection");
            }
            catch (DebeziumException expected) {
                assertThat(expected.getMessage()).contains("unmapped or ambiguous");
            }
            assertThat(processor.getTransactionCache().get("abcdef")).isNotNull();
            Mockito.verify(offsetContext, Mockito.never()).setScn(Mockito.any());
            assertThat(offsetContext.getCommitScn().getMaxCommittedScn()).isEqualTo(Scn.NULL);
        }
    }

    @Test
    public void shouldRejectMissingTransportMarkerBeforeApplyingAnyRow() throws Exception {
        LogMinerDictionaryRecoveryTest.Database db = new LogMinerDictionaryRecoveryTest.Database();
        configureOutput(db, new String[]{ boundaryLine(110, 36), "" });
        try (TestableMemoryLogMinerEventProcessor processor = plSqlProcessor()) {
            processor.handleStart(getStartLogMinerEventRow(Scn.valueOf(90), "abcdef"));
            processor.handleDataEvent(getInsertLogMinerEventRow(Scn.valueOf(91), "abcdef"));
            try {
                processor.process(partition, Scn.valueOf(100), Scn.valueOf(120));
                org.junit.Assert.fail("Expected incomplete output rejection");
            }
            catch (DebeziumException expected) {
                assertThat(expected.getMessage()).contains("Missing DBMS_OUTPUT");
            }
            assertThat(processor.getTransactionCache().get("abcdef")).isNotNull();
            Mockito.verify(offsetContext, Mockito.never()).setScn(Mockito.any());
            assertThat(db.starts).isEmpty();
        }
    }

    @Test
    public void shouldRereadFilteredZeroWindowAfterIndependentVerification() throws Exception {
        LogMinerDictionaryRecoveryTest.Database db = new LogMinerDictionaryRecoveryTest.Database(LogMinerDictionaryRecoveryTest.row(110, 6, null));
        configureOutput(db, new String[]{ "@END|120|complete" }, new String[]{ "@END|120|complete" });
        try (TestableMemoryLogMinerEventProcessor processor = plSqlProcessor()) {
            assertThat(processor.process(partition, Scn.valueOf(100), Scn.valueOf(120))).isEqualTo(Scn.valueOf(120));
            assertThat(db.countModes).containsExactly("raw", "online");
            Mockito.verify(offsetContext).setScn(Scn.valueOf(120));
        }
    }

    @Test
    public void shouldRejectRepeatedZeroOutputWhenRawRedoContainsCommit() throws Exception {
        LogMinerDictionaryRecoveryTest.Database db = new LogMinerDictionaryRecoveryTest.Database(LogMinerDictionaryRecoveryTest.row(110, 7, null));
        configureOutput(db, new String[]{ "@END|120|complete" }, new String[]{ "@END|120|complete" });
        try (TestableMemoryLogMinerEventProcessor processor = plSqlProcessor()) {
            try {
                processor.process(partition, Scn.valueOf(100), Scn.valueOf(120));
                org.junit.Assert.fail("Expected missing online commit rejection");
            }
            catch (DebeziumException expected) {
                assertThat(expected.getMessage()).contains("raw redo requires an online row");
            }
            Mockito.verify(offsetContext, Mockito.never()).setScn(Mockito.any());
        }
    }

    @Test
    public void shouldRecoverExplicitNoDataFoundBeforeReadingOutput() throws Exception {
        LogMinerDictionaryRecoveryTest.Database db = new LogMinerDictionaryRecoveryTest.Database();
        PreparedStatement query = configureOutput(db);
        Mockito.when(query.execute()).thenThrow(new SQLException("ORA-01403", "", 1403));
        try (TestableMemoryLogMinerEventProcessor processor = plSqlProcessor()) {
            assertThat(processor.process(partition, Scn.valueOf(100), Scn.valueOf(120))).isEqualTo(Scn.valueOf(120));
            assertThat(db.countModes).containsExactly("raw");
        }
    }

    @Test
    public void shouldHonorTruncatedCompleteScnWithoutReadingPastIt() throws Exception {
        LogMinerDictionaryRecoveryTest.Database db = new LogMinerDictionaryRecoveryTest.Database();
        configureOutput(db, new String[]{ boundaryLine(110, 36), "", "@END|110|truncated" });
        try (TestableMemoryLogMinerEventProcessor processor = plSqlProcessor()) {
            assertThat(processor.process(partition, Scn.valueOf(100), Scn.valueOf(120))).isEqualTo(Scn.valueOf(110));
            Mockito.verify(offsetContext, Mockito.never()).setScn(Scn.valueOf(120));
            assertThat(db.starts).isEmpty();
        }
    }

    private TestableMemoryLogMinerEventProcessor plSqlProcessor() {
        return getTestableProcessor(new OracleConnectorConfig(getConfig()
                .with(OracleConnectorConfig.LOG_MINING_STRATEGY, OracleConnectorConfig.LogMiningStrategy.PLSQL_OUTPUT)
                .build()));
    }

    @Test
    public void shouldCloseDiagnosticBeforeDispatchAndAuditPreservedCommit() throws Exception {
        LogMinerDictionaryRecoveryTest.Database db = new LogMinerDictionaryRecoveryTest.Database(
                LogMinerDictionaryRecoveryTest.row(101, 1, "9876"), LogMinerDictionaryRecoveryTest.row(101, 7, null));
        db.logFiles = new ArrayList<>(db.logFiles);
        db.logFiles.add(LogMinerDictionaryRecoveryTest.logFile("future", 2, 130, 150, 1, 3));
        configureOutput(db, new String[]{ "@END|120|complete" });
        Mockito.doAnswer(invocation -> {
            Mockito.verify(db.diagnosticConnection).close();
            assertThat(db.diagnosticSessionAllocated).isFalse();
            return null;
        }).when(dispatcher).dispatchDataChangeEvent(Mockito.any(), Mockito.any(), Mockito.any());
        LogInterceptor logs = new LogInterceptor(AbstractLogMinerEventProcessor.class);
        try (TestableMemoryLogMinerEventProcessor processor = plSqlProcessor()) {
            processor.handleDataEvent(getInsertLogMinerEventRow(Scn.valueOf(91), "abcdef"));
            assertThat(processor.process(partition, Scn.valueOf(100), Scn.valueOf(120))).isEqualTo(Scn.valueOf(101));
            assertThat(processor.isLogFileListChanged()).isTrue();
            assertThat(db.registeredOnlineFiles).containsExactly("archive-a", "archive-b");
            Mockito.verify(dispatcher).dispatchDataChangeEvent(Mockito.any(), Mockito.any(), Mockito.any());
            Mockito.verify(db.connection, Mockito.never()).close();
            assertThat(offsetContext.getCommitScn().getMaxCommittedScn()).isEqualTo(Scn.valueOf(101));
            assertThat(logs.containsErrorMessage("LOGMINER_SYS_REDO_DISCARD_PREPARED recoveryId=")).isTrue();
            assertThat(logs.containsErrorMessage("discardedSysDml=1")).isTrue();
            assertThat(logs.containsErrorMessage("logFiles=[archive-a, archive-b]")).isTrue();
            assertThat(logs.containsErrorMessage("action=DISCARD_CONFIRMED_SYS_DML")).isTrue();
            assertThat(logs.containsErrorMessage("action=PRESERVE_TRANSACTION_BOUNDARY")).isTrue();
            assertThat(logs.containsErrorMessage("LOGMINER_SYS_REDO_BOUNDARY_HANDLED recoveryId=")).isTrue();
            assertThat(logs.containsErrorMessage("LOGMINER_SYS_REDO_DISCARD_COMPLETED recoveryId=")).isTrue();
            assertThat(logs.containsErrorMessage("durableCheckpointNotConfirmed=true")).isTrue();
        }
        finally {
            logs.stop();
        }
    }

    @Test
    public void shouldRetainReloadSignalAcrossRecursiveReplayAndResetOnNextWindow() throws Exception {
        LogMinerDictionaryRecoveryTest.Database db = new LogMinerDictionaryRecoveryTest.Database(LogMinerDictionaryRecoveryTest.row(110, 1, "9876"));
        db.logFiles = new ArrayList<>(db.logFiles);
        db.logFiles.add(LogMinerDictionaryRecoveryTest.logFile("future", 2, 130, 150, 1, 3));
        configureOutput(db, new String[]{ "@END|120|complete" }, new String[]{ "@END|109|complete" },
                new String[]{ boundaryLine(120, 36), "", "@END|120|complete" });
        try (TestableMemoryLogMinerEventProcessor processor = plSqlProcessor()) {
            assertThat(processor.process(partition, Scn.valueOf(100), Scn.valueOf(120))).isEqualTo(Scn.valueOf(109));
            assertThat(processor.isLogFileListChanged()).isTrue();
            assertThat(db.starts).containsExactly("raw:101:120", "online:101:109", "raw:101:109");
            assertThat(processor.process(partition, Scn.valueOf(109), Scn.valueOf(120))).isEqualTo(Scn.valueOf(120));
            assertThat(processor.isLogFileListChanged()).isFalse();
        }
    }

    @Test
    public void shouldKeepTransactionAndOffsetsWhenRequiredArchiveIsUnavailable() throws Exception {
        LogMinerDictionaryRecoveryTest.Database db = new LogMinerDictionaryRecoveryTest.Database(LogMinerDictionaryRecoveryTest.row(101, 1, "9876"));
        db.logFiles.get(0).put("STATUS", 4);
        db.logFiles.get(0).put("INFO", "MISSING_LOGFILE");
        configureOutput(db, new String[]{ "@END|120|complete" });
        try (TestableMemoryLogMinerEventProcessor processor = plSqlProcessor()) {
            processor.handleDataEvent(getInsertLogMinerEventRow(Scn.valueOf(91), "abcdef"));
            try {
                processor.process(partition, Scn.valueOf(100), Scn.valueOf(120));
                org.junit.Assert.fail("Expected required log rejection");
            }
            catch (DebeziumException expected) {
                assertThat(expected.getMessage()).contains("REQUIRED_FILE_UNAVAILABLE");
            }
            assertThat(processor.getTransactionCache().get("abcdef")).isNotNull();
            assertThat(processor.isLogFileListChanged()).isFalse();
            Mockito.verify(offsetContext, Mockito.never()).setScn(Mockito.any());
            Mockito.verify(db.diagnosticConnection).close();
            assertThat(db.starts).isEmpty();
        }
    }

    @Test
    public void shouldCloseBAndKeepOffsetsWhenDiagnosticCleanupFails() throws Exception {
        LogMinerDictionaryRecoveryTest.Database db = new LogMinerDictionaryRecoveryTest.Database(
                LogMinerDictionaryRecoveryTest.row(101, 1, "9876"), LogMinerDictionaryRecoveryTest.row(101, 7, null));
        db.failDiagnosticEnd = true;
        configureOutput(db, new String[]{ "@END|120|complete" });
        LogInterceptor logs = new LogInterceptor(AbstractLogMinerEventProcessor.class);
        try (TestableMemoryLogMinerEventProcessor processor = plSqlProcessor()) {
            processor.handleDataEvent(getInsertLogMinerEventRow(Scn.valueOf(91), "abcdef"));
            try {
                processor.process(partition, Scn.valueOf(100), Scn.valueOf(120));
                org.junit.Assert.fail("Expected cleanup failure before metadata application");
            }
            catch (SQLException expected) {
                assertThat(expected.getMessage()).contains("diagnostic end failed");
            }
            Mockito.verify(db.diagnosticConnection).close();
            Mockito.verify(db.connection, Mockito.never()).close();
            Mockito.verify(offsetContext, Mockito.never()).setScn(Mockito.any());
            assertThat(processor.getTransactionCache().get("abcdef")).isNotNull();
            assertThat(logs.containsErrorMessage("LOGMINER_RECOVERY_NOT_APPLIED")).isTrue();
            assertThat(logs.containsErrorMessage("LOGMINER_SYS_REDO_DISCARD_COMPLETED")).isFalse();
        }
        finally {
            logs.stop();
        }
    }

    @Test
    public void shouldKeepTransactionAndOffsetWhenRootDictionaryQueryFails() throws Exception {
        LogMinerDictionaryRecoveryTest.Database db = new LogMinerDictionaryRecoveryTest.Database(
                LogMinerDictionaryRecoveryTest.row(101, 1, "9876"), LogMinerDictionaryRecoveryTest.row(101, 7, null));
        db.failRootDictionary = true;
        configureOutput(db, new String[]{ "@END|120|complete" });
        LogInterceptor logs = new LogInterceptor(AbstractLogMinerEventProcessor.class);
        try (TestableMemoryLogMinerEventProcessor processor = plSqlProcessor()) {
            processor.handleDataEvent(getInsertLogMinerEventRow(Scn.valueOf(91), "abcdef"));
            try {
                processor.process(partition, Scn.valueOf(100), Scn.valueOf(120));
                org.junit.Assert.fail("Expected root dictionary failure before commit application");
            }
            catch (SQLException expected) {
                assertThat(expected.getErrorCode()).isEqualTo(942);
            }
            Mockito.verify(db.diagnosticConnection).close();
            Mockito.verify(db.connection, Mockito.never()).close();
            Mockito.verify(offsetContext, Mockito.never()).setScn(Mockito.any());
            Mockito.verify(dispatcher, Mockito.never()).dispatchDataChangeEvent(Mockito.any(), Mockito.any(), Mockito.any());
            assertThat(processor.getTransactionCache().get("abcdef")).isNotNull();
            assertThat(logs.containsErrorMessage("LOGMINER_RECOVERY_NOT_APPLIED")).isTrue();
            assertThat(logs.containsErrorMessage("LOGMINER_SYS_REDO_DISCARD_PREPARED")).isFalse();
            assertThat(db.starts).containsExactly("raw:101:120");
        }
        finally {
            logs.stop();
        }
    }

    @Test
    public void shouldRecoverConsecutiveSysScnsAndLaterCommitThroughMainProcessor() throws Exception {
        LogMinerDictionaryRecoveryTest.Database db = new LogMinerDictionaryRecoveryTest.Database(
                LogMinerDictionaryRecoveryTest.row(101, 1, "9876"), LogMinerDictionaryRecoveryTest.row(102, 1, "9876"),
                LogMinerDictionaryRecoveryTest.row(110, 1, "9876"), LogMinerDictionaryRecoveryTest.row(110, 7, null));
        configureOutput(db, new String[]{ "@END|120|complete" }, new String[]{ "@END|120|complete" },
                new String[]{ "@END|120|complete" }, new String[]{ "@END|109|complete" }, new String[]{ "@END|120|complete" });
        try (TestableMemoryLogMinerEventProcessor processor = plSqlProcessor()) {
            processor.handleDataEvent(getInsertLogMinerEventRow(Scn.valueOf(91), "abcdef"));
            Scn next = processor.process(partition, Scn.valueOf(100), Scn.valueOf(120));
            assertThat(next).isEqualTo(Scn.valueOf(101));
            assertThat(processor.getTransactionCache().get("abcdef")).isNotNull();
            next = processor.process(partition, next, Scn.valueOf(120));
            assertThat(next).isEqualTo(Scn.valueOf(102));
            next = processor.process(partition, next, Scn.valueOf(120));
            assertThat(next).isEqualTo(Scn.valueOf(109));
            next = processor.process(partition, next, Scn.valueOf(120));
            assertThat(next).isEqualTo(Scn.valueOf(110));
            assertThat(processor.getTransactionCache()).isEmpty();
            assertThat(offsetContext.getCommitScn().getMaxCommittedScn()).isEqualTo(Scn.valueOf(110));
            Mockito.verify(db.diagnosticConnection, Mockito.times(5)).close();
            Mockito.verify(db.connection, Mockito.never()).close();
        }
    }

    @Test
    public void shouldNotLogCompletedDiscardWhenHeartbeatFails() throws Exception {
        LogMinerDictionaryRecoveryTest.Database db = new LogMinerDictionaryRecoveryTest.Database(LogMinerDictionaryRecoveryTest.row(101, 1, "9876"));
        configureOutput(db, new String[]{ "@END|120|complete" });
        Mockito.doThrow(new InterruptedException("heartbeat interrupted")).when(dispatcher).dispatchHeartbeatEvent(Mockito.any(), Mockito.any());
        LogInterceptor logs = new LogInterceptor(AbstractLogMinerEventProcessor.class);
        try (TestableMemoryLogMinerEventProcessor processor = plSqlProcessor()) {
            try {
                processor.process(partition, Scn.valueOf(100), Scn.valueOf(120));
                org.junit.Assert.fail("Expected heartbeat interruption");
            }
            catch (InterruptedException expected) {
                assertThat(expected.getMessage()).contains("heartbeat interrupted");
            }
            assertThat(logs.containsErrorMessage("LOGMINER_SYS_REDO_APPLY_FAILED")).isTrue();
            assertThat(logs.containsErrorMessage("LOGMINER_SYS_REDO_DISCARD_COMPLETED")).isFalse();
            Mockito.verify(db.diagnosticConnection).close();
        }
        finally {
            logs.stop();
        }
    }

    private PreparedStatement configureOutput(LogMinerDictionaryRecoveryTest.Database db, String[]... batches) throws Exception {
        connection = Mockito.mock(OracleConnection.class);
        Mockito.when(connection.connection()).thenReturn(db.connection);
        diagnosticConnection = Mockito.mock(OracleConnection.class);
        Mockito.when(diagnosticConnection.connection()).thenReturn(db.diagnosticConnection);
        Mockito.doAnswer(invocation -> {
            db.diagnosticConnection.close();
            return null;
        }).when(diagnosticConnection).close();
        Mockito.when(offsetContext.getScn()).thenReturn(Scn.valueOf(100));
        Mockito.when(offsetContext.getSnapshotScn()).thenReturn(Scn.valueOf(0));
        final PreparedStatement query = Mockito.mock(PreparedStatement.class);
        Mockito.when(query.getConnection()).thenReturn(db.connection);
        Mockito.when(db.connection.prepareStatement(anyString(), Mockito.anyInt(), Mockito.anyInt(), Mockito.anyInt())).thenReturn(query);
        Deque<String[]> pending = new ArrayDeque<>(Arrays.asList(batches));
        AtomicReference<String[]> output = new AtomicReference<>();
        Mockito.when(query.execute()).thenAnswer(invocation -> {
            output.set(pending.isEmpty() ? new String[0] : pending.removeFirst());
            return false;
        });
        OracleCallableStatement batch = Mockito.mock(OracleCallableStatement.class);
        Mockito.when(batch.unwrap(OracleCallableStatement.class)).thenReturn(batch);
        AtomicReference<String[]> fetched = new AtomicReference<>();
        Mockito.when(batch.execute()).thenAnswer(invocation -> {
            fetched.set(output.getAndSet(new String[0]));
            return false;
        });
        Mockito.when(batch.getInt(2)).thenAnswer(invocation -> fetched.get().length);
        Mockito.when(batch.getPlsqlIndexTable(1)).thenAnswer(invocation -> fetched.get());
        Mockito.when(db.connection.prepareCall("BEGIN DBMS_OUTPUT.GET_LINES(?, ?); END;")).thenReturn(batch);
        return query;
    }

    private String boundaryLine(long scn, int code) {
        return "@ROW|" + scn + "|" + code + "|2026-09-08 01:00:00|abcdef|0|||" + (code == 7 ? "COMMIT" : "ROLLBACK")
                + "||0|0|0x0001.0002.0003|0|1|1|";
    }

    private TestableMemoryLogMinerEventProcessor getTestableProcessor(OracleConnectorConfig connectorConfig) {
        assertThat(connectorConfig.validateAndRecord(OracleConnectorConfig.ALL_FIELDS, LOGGER::error)).isTrue();
        TestableMemoryLogMinerEventProcessor processor = new TestableMemoryLogMinerEventProcessor(context,
                connectorConfig,
                connection,
                dispatcher,
                partition,
                offsetContext,
                schema,
                metrics);
        processor.diagnosticConnection = diagnosticConnection;
        return processor;
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
        private OracleConnection diagnosticConnection;

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

        @Override
        public java.util.Map<String, io.debezium.connector.oracle.logminer.processor.memory.MemoryTransaction> getTransactionCache() {
            return super.getTransactionCache();
        }

        @Override
        protected OracleConnection createDiagnosticConnection() {
            if (diagnosticConnection == null) {
                throw new AssertionError("Unexpected diagnostic connection request");
            }
            return diagnosticConnection;
        }

        void abandonTransactionsAfterEmptyPlSqlOutputWindowForTest(Scn endScn) throws InterruptedException {
            super.abandonTransactionsAfterEmptyPlSqlOutputWindow(endScn);
        }
    }
}
