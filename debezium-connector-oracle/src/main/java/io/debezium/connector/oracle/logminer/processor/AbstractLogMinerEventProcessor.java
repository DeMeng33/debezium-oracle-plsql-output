/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.processor;

import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnection.NonRelationalTableException;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleDatabaseSchema;
import io.debezium.connector.oracle.OracleOffsetContext;
import io.debezium.connector.oracle.OraclePartition;
import io.debezium.connector.oracle.OracleSchemaChangeEventEmitter;
import io.debezium.connector.oracle.OracleStreamingChangeEventSourceMetrics;
import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.logminer.LogMinerChangeRecordEmitter;
import io.debezium.connector.oracle.logminer.events.DmlEvent;
import io.debezium.connector.oracle.logminer.events.EventType;
import io.debezium.connector.oracle.logminer.events.LobEraseEvent;
import io.debezium.connector.oracle.logminer.events.LobWriteEvent;
import io.debezium.connector.oracle.logminer.events.LogMinerEvent;
import io.debezium.connector.oracle.logminer.events.LogMinerEventRow;
import io.debezium.connector.oracle.logminer.events.SelectLobLocatorEvent;
import io.debezium.connector.oracle.logminer.events.TruncateEvent;
import io.debezium.connector.oracle.logminer.parser.DmlParserException;
import io.debezium.connector.oracle.logminer.parser.LogMinerDmlEntry;
import io.debezium.connector.oracle.logminer.parser.LogMinerDmlEntryImpl;
import io.debezium.connector.oracle.logminer.parser.LogMinerDmlParser;
import io.debezium.connector.oracle.logminer.parser.SelectLobParser;
import io.debezium.data.Envelope;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.ChangeEventSource.ChangeEventSourceContext;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;
import io.debezium.util.Strings;

import oracle.jdbc.OracleCallableStatement;
import oracle.jdbc.OracleTypes;

/**
 * An abstract implementation of {@link LogMinerEventProcessor} that all processors should extend.
 *
 * @author Chris Cranford
 */
public abstract class AbstractLogMinerEventProcessor<T extends AbstractTransaction> implements LogMinerEventProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractLogMinerEventProcessor.class);
    private static final String NO_SEQUENCE_TRX_ID_SUFFIX = "ffffffff";
    private static final String PLSQL_OUTPUT_ROW_PREFIX = "@ROW|";
    private static final String PLSQL_OUTPUT_END_PREFIX = "@END|";
    private static final DateTimeFormatter PLSQL_OUTPUT_TIMESTAMP_FORMAT = new DateTimeFormatterBuilder()
            .appendPattern("yyyy-MM-dd HH:mm:ss")
            .optionalStart()
            .appendFraction(ChronoField.NANO_OF_SECOND, 1, 9, true)
            .optionalEnd()
            .toFormatter();
    private static final Duration PLSQL_OUTPUT_IDLE_LOG_INTERVAL = Duration.ofSeconds(30);
    private static final Duration PLSQL_OUTPUT_READ_PROGRESS_LOG_INTERVAL = Duration.ofSeconds(15);
    private static final int PLSQL_OUTPUT_GET_LINES_BATCH_SIZE = 500;
    private static final int[] PLSQL_OUTPUT_GET_LINES_LINE_LENGTHS = { 8000, 4000 };
    private static final int PLSQL_OUTPUT_LOG_PREVIEW_LENGTH = 512;
    private static final int PLSQL_OUTPUT_SAMPLE_LIMIT = 5;

    private final ChangeEventSourceContext context;
    private final OracleConnectorConfig connectorConfig;
    private final OracleDatabaseSchema schema;
    private final OraclePartition partition;
    private final OracleOffsetContext offsetContext;
    private final EventDispatcher<OraclePartition, TableId> dispatcher;
    private final OracleStreamingChangeEventSourceMetrics metrics;
    private final LogMinerDmlParser dmlParser;
    private final SelectLobParser selectLobParser;

    protected final Counters counters;

    private Scn currentOffsetScn = Scn.NULL;
    private Map<Integer, Scn> currentOffsetCommitScns = new HashMap<>();
    private Scn lastProcessedScn = Scn.NULL;
    private boolean sequenceUnavailable = false;
    private Instant lastPlSqlOutputActivityLogTime = Instant.EPOCH;
    private long plSqlOutputEmptyBatchCount;

    public AbstractLogMinerEventProcessor(ChangeEventSourceContext context,
                                          OracleConnectorConfig connectorConfig,
                                          OracleDatabaseSchema schema,
                                          OraclePartition partition,
                                          OracleOffsetContext offsetContext,
                                          EventDispatcher<OraclePartition, TableId> dispatcher,
                                          OracleStreamingChangeEventSourceMetrics metrics) {
        this.context = context;
        this.connectorConfig = connectorConfig;
        this.schema = schema;
        this.partition = partition;
        this.offsetContext = offsetContext;
        this.dispatcher = dispatcher;
        this.metrics = metrics;
        this.counters = new Counters();
        this.dmlParser = new LogMinerDmlParser();
        this.selectLobParser = new SelectLobParser();
    }

    protected OracleConnectorConfig getConfig() {
        return connectorConfig;
    }

    protected OracleDatabaseSchema getSchema() {
        return schema;
    }

    /**
     * Check whether a transaction has been recently processed through either a commit or rollback.
     *
     * @param transactionId the unique transaction id
     * @return true if the transaction has been recently processed, false otherwise
     */
    protected boolean isRecentlyProcessed(String transactionId) {
        return false;
    }

    /**
     * Checks whether the LogMinerEvent row for a schema change can be emitted.
     *
     * @param row the result set row
     * @return true if the schema change has been seen, false otherwise.
     */
    protected boolean hasSchemaChangeBeenSeen(LogMinerEventRow row) {
        return false;
    }

    /**
     * Return the last processed system change number handled by the processor.
     *
     * @return the last processed system change number, never {@code null}.
     */
    protected Scn getLastProcessedScn() {
        return lastProcessedScn;
    }

    /**
     * Returns the {@code TransactionCache} implementation.
     * @return the transaction cache, never {@code null}
     */
    protected abstract Map<String, T> getTransactionCache();

    /**
     * Creates a new transaction based on the supplied {@code START} event.
     *
     * @param row the event row, must not be {@code null}
     * @return the implementation-specific {@link Transaction} instance
     */
    protected abstract T createTransaction(LogMinerEventRow row);

    /**
     * Removes a specific transaction event by database row identifier.
     *
     * @param row the event row that contains the row identifier, must not be {@code null}
     */
    protected abstract void removeEventWithRowId(LogMinerEventRow row);

    /**
     * Returns the number of events associated with the specified transaction.
     *
     * @param transaction the transaction, must not be {@code null}
     * @return the number of events in the transaction
     */
    protected abstract int getTransactionEventCount(T transaction);

    // todo: can this be removed in favor of a single implementation?
    protected boolean isTrxIdRawValue() {
        return true;
    }

    @Override
    public Scn process(OraclePartition partition, Scn startScn, Scn endScn) throws SQLException, InterruptedException {
        counters.reset();

        if (OracleConnectorConfig.LogMiningStrategy.PLSQL_OUTPUT.equals(getConfig().getLogMiningStrategy())) {
            return processPlSqlOutput(partition, startScn, endScn);
        }

        try (PreparedStatement statement = createQueryStatement()) {
            LOGGER.debug("Fetching results for SCN [{}, {}]", startScn, endScn);
            statement.setFetchSize(getConfig().getLogMiningViewFetchSize());
            statement.setFetchDirection(ResultSet.FETCH_FORWARD);
            statement.setString(1, startScn.toString());
            statement.setString(2, endScn.toString());

            Instant queryStart = Instant.now();
            try (ResultSet resultSet = statement.executeQuery()) {
                metrics.setLastDurationOfBatchCapturing(Duration.between(queryStart, Instant.now()));

                Instant startProcessTime = Instant.now();
                processResults(this.partition, resultSet);

                Duration totalTime = Duration.between(startProcessTime, Instant.now());
                metrics.setLastCapturedDmlCount(counters.dmlCount);

                if (counters.dmlCount > 0 || counters.commitCount > 0 || counters.rollbackCount > 0) {
                    warnPotentiallyStuckScn(currentOffsetScn, currentOffsetCommitScns);

                    currentOffsetScn = offsetContext.getScn();
                    if (offsetContext.getCommitScn() != null) {
                        currentOffsetCommitScns = offsetContext.getCommitScn().getCommitScnForAllRedoThreads();
                    }
                }

                LOGGER.debug("{}.", counters);
                LOGGER.debug("Processed in {} ms. Lag: {}. Offset SCN: {}, Offset Commit SCN: {}, Active Transactions: {}, Sleep: {}",
                        totalTime.toMillis(), metrics.getLagFromSourceInMilliseconds(), offsetContext.getScn(),
                        offsetContext.getCommitScn(), metrics.getNumberOfActiveTransactions(),
                        metrics.getMillisecondToSleepBetweenMiningQuery());

                metrics.addProcessedRows(counters.rows);
                return calculateNewStartScn(endScn, offsetContext.getCommitScn().getMaxCommittedScn());
            }
        }
    }

    /**
     * Create the JDBC query that will be used to fetch the mining result set.
     *
     * @return a prepared query statement, never {@code null}
     * @throws SQLException if a database exception occurred creating the statement
     */
    protected abstract PreparedStatement createQueryStatement() throws SQLException;

    /**
     * Gets the Oracle connection used by the processor.
     *
     * @return the Oracle connection, never {@code null}
     */
    protected abstract OracleConnection getJdbcConnection();

    private Scn processPlSqlOutput(OraclePartition partition, Scn startScn, Scn endScn) throws SQLException, InterruptedException {
        try (PreparedStatement statement = createQueryStatement()) {
            maybeLogPlSqlOutputPolling(startScn, endScn);
            LOGGER.debug("Fetching PL/SQL output results for SCN [{}, {}]", startScn, endScn);
            statement.setString(1, startScn.toString());
            statement.setString(2, endScn.toString());

            Instant queryStart = Instant.now();
            LOGGER.debug("PL/SQL output LogMiner executing DBMS_OUTPUT query block: scnRange=[{}, {}], offsetScn={}, offsetCommitScn={}, activeTransactions={}",
                    startScn, endScn, offsetContext.getScn(), offsetContext.getCommitScn(),
                    metrics.getNumberOfActiveTransactions());
            statement.execute();
            Duration queryDuration = Duration.between(queryStart, Instant.now());
            metrics.setLastDurationOfBatchCapturing(queryDuration);
            LOGGER.debug("PL/SQL output LogMiner DBMS_OUTPUT query block finished in {} ms: scnRange=[{}, {}]",
                    queryDuration.toMillis(), startScn, endScn);

            Instant startProcessTime = Instant.now();
            long rowsBefore = counters.rows;
            int dmlBefore = counters.dmlCount;
            int commitBefore = counters.commitCount;
            int rollbackBefore = counters.rollbackCount;
            PlSqlOutputReadStats readStats = processPlSqlOutputRows(partition, statement, startScn, endScn);
            LOGGER.debug(
                    "PL/SQL output LogMiner DBMS_OUTPUT rows read: scnRange=[{}, {}], outputLines={}, outputFetches={}, outputRows={}, completedRows={}, truncated={}, effectiveEndScn={}, operations={}, lastRow={}",
                    startScn, endScn, readStats.outputLines, readStats.outputFetches, readStats.outputRows, readStats.completedRows,
                    readStats.truncated, readStats.getEffectiveEndScn(endScn), readStats.describeOperationCounts(), readStats.describeLastRow());

            Duration totalTime = Duration.between(startProcessTime, Instant.now());
            metrics.setLastCapturedDmlCount(counters.dmlCount);

            if (counters.dmlCount > 0 || counters.commitCount > 0 || counters.rollbackCount > 0) {
                warnPotentiallyStuckScn(currentOffsetScn, currentOffsetCommitScns);

                currentOffsetScn = offsetContext.getScn();
                if (offsetContext.getCommitScn() != null) {
                    currentOffsetCommitScns = offsetContext.getCommitScn().getCommitScnForAllRedoThreads();
                }
            }

            logPlSqlOutputBatch(startScn, endScn, queryDuration, totalTime, readStats,
                    counters.rows - rowsBefore,
                    counters.dmlCount - dmlBefore,
                    counters.commitCount - commitBefore,
                    counters.rollbackCount - rollbackBefore);

            LOGGER.debug("{}.", counters);
            LOGGER.debug("Processed PL/SQL output in {} ms. Lag: {}. Offset SCN: {}, Offset Commit SCN: {}, Active Transactions: {}, Sleep: {}",
                    totalTime.toMillis(), metrics.getLagFromSourceInMilliseconds(), offsetContext.getScn(),
                    offsetContext.getCommitScn(), metrics.getNumberOfActiveTransactions(),
                    metrics.getMillisecondToSleepBetweenMiningQuery());

            metrics.addProcessedRows(counters.rows - rowsBefore);
            if (readStats.outputRows == 0 && !readStats.truncated) {
                final Scn advancedScn = advanceAfterEmptyPlSqlOutputWindow(partition, startScn, endScn);
                if (advancedScn != null) {
                    return advancedScn;
                }
            }
            final Scn effectiveEndScn = readStats.getEffectiveEndScn(endScn);
            return calculateNewStartScn(effectiveEndScn, offsetContext.getCommitScn().getMaxCommittedScn());
        }
    }

    private Scn advanceAfterEmptyPlSqlOutputWindow(OraclePartition partition, Scn startScn, Scn endScn)
            throws InterruptedException {
        final int activeTransactionsBefore = getTransactionCache().size();
        if (activeTransactionsBefore == 0) {
            advanceOffsetAfterEmptyPlSqlOutputWindow(partition, endScn);
            LOGGER.info("PL/SQL output LogMiner advanced empty window: scnRange=[{}, {}], offsetScn={}, activeTransactions=0",
                    startScn, endScn, offsetContext.getScn());
            return endScn;
        }

        final Scn minTransactionScnBefore = getTransactionCacheMinimumScn();
        final String transactionIdsBefore = describeCachedTransactionIds();
        maybeLogEmptyPlSqlOutputWindowWithActiveTransactions(startScn, endScn, activeTransactionsBefore,
                minTransactionScnBefore, transactionIdsBefore);

        final Scn terminalScn = abandonTransactionsAfterEmptyPlSqlOutputWindow(partition, endScn);

        final int activeTransactionsAfter = getTransactionCache().size();
        if (activeTransactionsAfter == 0) {
            // If a cached transaction was recovered from its terminal LogMiner row, advance only to that
            // terminal SCN. The rest of the empty window must be mined again to avoid skipping changes.
            final Scn advanceScn = terminalScn != null && !terminalScn.isNull() ? terminalScn : endScn;
            advanceOffsetAfterEmptyPlSqlOutputWindow(partition, advanceScn);
            LOGGER.info(
                    "PL/SQL output LogMiner abandoned or recovered stale transaction(s) after empty window and advanced offset: scnRange=[{}, {}], advancedScn={}, offsetScn={}, activeTransactionsBefore={}, activeTransactionsAfter={}, minTransactionScnBefore={}, transactionIdsBefore={}, retention={}",
                    startScn, endScn, advanceScn, offsetContext.getScn(), activeTransactionsBefore, activeTransactionsAfter,
                    minTransactionScnBefore, transactionIdsBefore, getConfig().getLogMiningTransactionRetention());
            return advanceScn;
        }
        return null;
    }

    private void advanceOffsetAfterEmptyPlSqlOutputWindow(OraclePartition partition, Scn endScn)
            throws InterruptedException {
        offsetContext.setScn(endScn);
        metrics.setOldestScn(endScn);
        metrics.setOffsetScn(endScn);
        dispatcher.dispatchHeartbeatEvent(partition, offsetContext);
    }

    protected Scn abandonTransactionsAfterEmptyPlSqlOutputWindow(OraclePartition partition, Scn endScn) throws InterruptedException {
        abandonTransactions(getConfig().getLogMiningTransactionRetention());
        return null;
    }

    private void maybeLogEmptyPlSqlOutputWindowWithActiveTransactions(Scn startScn, Scn endScn, int activeTransactions,
                                                                      Scn minTransactionScn, String transactionIds) {
        final Instant now = Instant.now();
        if (Duration.between(lastPlSqlOutputActivityLogTime, now).compareTo(PLSQL_OUTPUT_IDLE_LOG_INTERVAL) >= 0) {
            LOGGER.info(
                    "PL/SQL output LogMiner empty window with active transactions: scnRange=[{}, {}], offsetScn={}, offsetCommitScn={}, activeTransactions={}, minTransactionScn={}, transactionIds={}, retention={}, emptyBatchesSinceLastLog={}",
                    startScn, endScn, offsetContext.getScn(), offsetContext.getCommitScn(), activeTransactions,
                    minTransactionScn, transactionIds, getConfig().getLogMiningTransactionRetention(), plSqlOutputEmptyBatchCount);
            lastPlSqlOutputActivityLogTime = now;
            plSqlOutputEmptyBatchCount = 0;
        }
    }

    private String describeCachedTransactionIds() {
        if (getTransactionCache().isEmpty()) {
            return "[]";
        }
        final StringBuilder result = new StringBuilder("[");
        int count = 0;
        for (String transactionId : getTransactionCache().keySet()) {
            if (count > 0) {
                result.append(", ");
            }
            if (count == PLSQL_OUTPUT_SAMPLE_LIMIT) {
                result.append("... total=").append(getTransactionCache().size());
                break;
            }
            result.append(transactionId);
            count++;
        }
        return result.append(']').toString();
    }

    private void maybeLogPlSqlOutputPolling(Scn startScn, Scn endScn) {
        final Instant now = Instant.now();
        if (Duration.between(lastPlSqlOutputActivityLogTime, now).compareTo(PLSQL_OUTPUT_IDLE_LOG_INTERVAL) >= 0) {
            LOGGER.info(
                    "PL/SQL output LogMiner polling: scnRange=[{}, {}], emptyBatchesSinceLastLog={}, offsetScn={}, offsetCommitScn={}, activeTransactions={}, sleepMs={}",
                    startScn, endScn, plSqlOutputEmptyBatchCount, offsetContext.getScn(), offsetContext.getCommitScn(),
                    metrics.getNumberOfActiveTransactions(), metrics.getMillisecondToSleepBetweenMiningQuery());
            lastPlSqlOutputActivityLogTime = now;
            plSqlOutputEmptyBatchCount = 0;
        }
    }

    private void logPlSqlOutputBatch(Scn startScn, Scn endScn, Duration queryDuration, Duration processDuration,
                                     PlSqlOutputReadStats readStats, long outputRows, int dmlCount,
                                     int commitCount, int rollbackCount) {
        // Foreign-transaction commit rows arrive every cycle; only log batches with captured work or a truncation.
        if (dmlCount > 0 || commitCount > 0 || rollbackCount > 0 || readStats.truncated) {
            LOGGER.info(
                    "PL/SQL output LogMiner batch: scnRange=[{}, {}], effectiveEndScn={}, truncated={}, outputLines={}, outputFetches={}, outputRows={}, completedRows={}, dml={}, commits={}, rollbacks={}, operations={}, queryMs={}, processMs={}, lastRow={}, offsetScn={}, offsetCommitScn={}, activeTransactions={}",
                    startScn, endScn, readStats.getEffectiveEndScn(endScn), readStats.truncated,
                    readStats.outputLines, readStats.outputFetches, outputRows, readStats.completedRows, dmlCount, commitCount, rollbackCount,
                    readStats.describeOperationCounts(), queryDuration.toMillis(), processDuration.toMillis(), readStats.describeLastRow(),
                    offsetContext.getScn(), offsetContext.getCommitScn(), metrics.getNumberOfActiveTransactions());
            lastPlSqlOutputActivityLogTime = Instant.now();
            plSqlOutputEmptyBatchCount = 0;
        }
        else {
            plSqlOutputEmptyBatchCount++;
        }
    }

    private PlSqlOutputReadStats processPlSqlOutputRows(OraclePartition partition, PreparedStatement queryStatement, Scn startScn, Scn endScn)
            throws SQLException, InterruptedException {
        PlSqlOutputReadStats stats = new PlSqlOutputReadStats();
        try (DbmsOutputLineReader reader = new DbmsOutputLineReader(queryStatement)) {
            PlSqlOutputRowBuilder current = null;
            Set<String> newTransactionsInBatch = new HashSet<>();
            Set<String> replayResetTransactions = new HashSet<>();
            Instant lastProgressLogTime = Instant.now();
            while (context.isRunning()) {
                DbmsOutputLine header = reader.readLine();
                stats.outputFetches = reader.getFetchCalls();
                stats.outputLines = reader.getFetchedLines();
                if (header.status != 0) {
                    break;
                }
                if (!Strings.isNullOrBlank(header.value) && header.value.startsWith(PLSQL_OUTPUT_END_PREFIX)) {
                    if (current != null) {
                        stats.flushedPartialRow = true;
                        throw new DebeziumException("DBMS_OUTPUT LogMiner end marker encountered before SQL_REDO continuation completed. lastRow=" +
                                stats.describeLastRow());
                    }
                    parsePlSqlOutputEnd(header.value, stats, startScn, endScn);
                    break;
                }
                if (Strings.isNullOrBlank(header.value) || !header.value.startsWith(PLSQL_OUTPUT_ROW_PREFIX)) {
                    throw new DebeziumException("Bad DBMS_OUTPUT LogMiner row header: " + logPreview(header.value));
                }

                DbmsOutputLine sqlRedo = reader.readLine();
                stats.outputFetches = reader.getFetchCalls();
                stats.outputLines = reader.getFetchedLines();
                if (sqlRedo.status != 0) {
                    throw new DebeziumException("Missing DBMS_OUTPUT SQL_REDO after header: " + logPreview(header.value));
                }

                PlSqlOutputRow row;
                try {
                    row = parsePlSqlOutputRow(header.value, sqlRedo.value);
                }
                catch (RuntimeException e) {
                    throw new DebeziumException("Failed to parse DBMS_OUTPUT LogMiner row. header=" + logPreview(header.value)
                            + ", sqlRedo=" + logPreview(sqlRedo.value), e);
                }
                counters.rows++;
                stats.outputRows++;
                stats.remember(row);
                if (LOGGER.isDebugEnabled() && stats.shouldLogSample(row)) {
                    LOGGER.debug(
                            "PL/SQL output LogMiner sample row: scn={}, operationCode={}, operation={}, owner={}, table={}, transactionId={}, csf={}, status={}, rollback={}, sqlRedo={}",
                            row.scn, row.operationCode, row.operation, row.tablespaceName, row.tableName, row.transactionId, row.csf, row.status,
                            row.rollbackFlag, logPreview(row.sqlRedo));
                }

                if (current == null) {
                    current = new PlSqlOutputRowBuilder(row);
                }
                else {
                    current.appendSqlRedo(row.sqlRedo);
                }

                if (row.csf == 0) {
                    LogMinerEventRow eventRow = current.build(getConfig().getCatalogName());
                    resetPlSqlOutputTransactionReplayCursor(eventRow, newTransactionsInBatch, replayResetTransactions);
                    processRow(partition, eventRow);
                    stats.completedRows++;
                    current = null;
                }

                final Instant now = Instant.now();
                if (Duration.between(lastProgressLogTime, now).compareTo(PLSQL_OUTPUT_READ_PROGRESS_LOG_INTERVAL) >= 0) {
                    LOGGER.info(
                            "PL/SQL output LogMiner reading progress: outputLines={}, outputFetches={}, outputRows={}, completedRows={}, operations={}, lastRow={}, activeTransactions={}",
                            stats.outputLines, stats.outputFetches, stats.outputRows, stats.completedRows,
                            stats.describeOperationCounts(), stats.describeLastRow(), metrics.getNumberOfActiveTransactions());
                    lastProgressLogTime = now;
                }
            }
            if (current != null) {
                stats.flushedPartialRow = true;
                throw new DebeziumException("Incomplete DBMS_OUTPUT LogMiner row detected at end of output. " +
                        "The SQL_REDO continuation was not fully read; refusing to advance the mining offset. lastRow=" +
                        stats.describeLastRow());
            }
        }
        return stats;
    }

    private void resetPlSqlOutputTransactionReplayCursor(LogMinerEventRow row, Set<String> newTransactionsInBatch,
                                                         Set<String> replayResetTransactions) {
        if (!isPlSqlOutputTransactionReplayCandidate(row)) {
            return;
        }
        final AbstractTransaction transaction = getTransactionCache().get(row.getTransactionId());
        if (transaction == null) {
            newTransactionsInBatch.add(row.getTransactionId());
            return;
        }
        if (newTransactionsInBatch.contains(row.getTransactionId())) {
            return;
        }
        if (transaction.getStartScn().equals(row.getScn()) && replayResetTransactions.add(row.getTransactionId())) {
            LOGGER.trace("PL/SQL output LogMiner transaction {} replay detected at SCN {}, resetting event cursor.",
                    row.getTransactionId(), row.getScn());
            transaction.start();
        }
    }

    private boolean isPlSqlOutputTransactionReplayCandidate(LogMinerEventRow row) {
        switch (row.getEventType()) {
            case INSERT:
            case UPDATE:
            case DELETE:
            case SELECT_LOB_LOCATOR:
            case LOB_WRITE:
            case LOB_ERASE:
                return row.getTransactionId() != null;
            default:
                return false;
        }
    }

    private PlSqlOutputRow parsePlSqlOutputRow(String line, String sqlRedo) {
        final String[] parts = line.split("\\|", 17);
        if (parts.length < 17) {
            throw new DebeziumException("Bad DBMS_OUTPUT LogMiner row header: " + line);
        }
        return new PlSqlOutputRow(
                Scn.valueOf(parts[1]),
                parseInt(parts[2]),
                parseTimestamp(parts[3]),
                nullIfEmpty(parts[4]),
                parseInt(parts[5]),
                nullIfEmpty(parts[6]),
                nullIfEmpty(parts[7]),
                nullIfEmpty(parts[8]),
                nullIfEmpty(parts[9]),
                nullIfEmpty(parts[10]),
                parseInt(parts[11]) == 1,
                nullIfEmpty(parts[12]),
                parseInt(parts[13]),
                parseInt(parts[14]),
                parseInt(parts[15]),
                nullIfEmpty(parts[16]),
                sqlRedo);
    }

    private void parsePlSqlOutputEnd(String line, PlSqlOutputReadStats stats, Scn startScn, Scn endScn) {
        final String[] parts = line.split("\\|", 3);
        if (parts.length != 3 || !"truncated".equals(parts[2])) {
            throw new DebeziumException("Bad DBMS_OUTPUT LogMiner end marker: " + logPreview(line));
        }
        if (Strings.isNullOrBlank(parts[1])) {
            throw new DebeziumException("DBMS_OUTPUT LogMiner end marker truncated before any row was completed.");
        }
        final Scn effectiveEndScn = Scn.valueOf(parts[1]);
        if (effectiveEndScn.compareTo(startScn) <= 0 || effectiveEndScn.compareTo(endScn) > 0) {
            throw new DebeziumException("DBMS_OUTPUT LogMiner end marker SCN is outside the mining window: markerScn=" +
                    effectiveEndScn + ", scnRange=[" + startScn + ", " + endScn + "]");
        }
        stats.truncated = true;
        stats.effectiveEndScn = effectiveEndScn;
    }

    private Instant parseTimestamp(String value) {
        if (Strings.isNullOrBlank(value)) {
            return null;
        }
        return LocalDateTime.parse(value, PLSQL_OUTPUT_TIMESTAMP_FORMAT).toInstant(ZoneOffset.UTC);
    }

    private int parseInt(String value) {
        return Strings.isNullOrBlank(value) ? 0 : Integer.parseInt(value);
    }

    private String nullIfEmpty(String value) {
        return Strings.isNullOrBlank(value) ? null : value;
    }

    private String logPreview(String value) {
        if (value == null) {
            return "<null>";
        }
        String sanitized = value.replace('\r', ' ').replace('\n', ' ');
        if (sanitized.length() <= PLSQL_OUTPUT_LOG_PREVIEW_LENGTH) {
            return sanitized;
        }
        return sanitized.substring(0, PLSQL_OUTPUT_LOG_PREVIEW_LENGTH) + "...";
    }

    protected TerminalTransactionEventLookup findTerminalTransactionEvent(String transactionId, Scn startScn, Scn endScn) {
        if (!isHexTransactionId(transactionId) || startScn.isNull() || endScn.isNull()) {
            LOGGER.warn("Unable to check terminal LogMiner event for transaction {}; invalid xid or SCN range. startScn={}, endScn={}", transactionId, startScn, endScn);
            metrics.incrementWarningCount();
            return TerminalTransactionEventLookup.failed();
        }

        final String block = "DECLARE " +
                "BEGIN " +
                "DBMS_OUTPUT.DISABLE; " +
                "DBMS_OUTPUT.ENABLE(1000000); " +
                "FOR r IN (" +
                "SELECT OPERATION_CODE, SCN, TIMESTAMP AS CHANGE_TIME, USERNAME, THREAD# AS THREAD_NUMBER " +
                "FROM V$LOGMNR_CONTENTS " +
                "WHERE XID = HEXTORAW(?) " +
                "AND OPERATION_CODE IN (?, ?) " +
                "AND SCN >= ? AND SCN <= ? " +
                "ORDER BY SCN" +
                ") LOOP " +
                "DBMS_OUTPUT.PUT_LINE('@TX|' || TO_CHAR(r.OPERATION_CODE) || '|' || TO_CHAR(r.SCN) || '|' || " +
                "TO_CHAR(r.CHANGE_TIME, 'YYYY-MM-DD HH24:MI:SS') || '|' || NVL(r.USERNAME, '') || '|' || TO_CHAR(NVL(r.THREAD_NUMBER, 0))); " +
                "EXIT; " +
                "END LOOP; " +
                "END;";

        try (CallableStatement statement = getJdbcConnection().connection().prepareCall(block)) {
            statement.setString(1, transactionId.toUpperCase());
            statement.setInt(2, EventType.COMMIT.getValue());
            statement.setInt(3, EventType.ROLLBACK.getValue());
            statement.setString(4, startScn.toString());
            statement.setString(5, endScn.toString());
            statement.execute();

            try (DbmsOutputLineReader reader = new DbmsOutputLineReader(statement)) {
                final DbmsOutputLine line = reader.readLine();
                if (line.status != 0 || Strings.isNullOrBlank(line.value)) {
                    return TerminalTransactionEventLookup.notFound();
                }
                return TerminalTransactionEventLookup.found(parseTerminalTransactionEvent(line.value));
            }
        }
        catch (SQLException e) {
            LOGGER.warn("Unable to check terminal LogMiner event for transaction {}; keeping it in cache.", transactionId, e);
            metrics.incrementWarningCount();
            return TerminalTransactionEventLookup.failed();
        }
        catch (RuntimeException e) {
            LOGGER.warn("Oracle JDBC failed while checking terminal LogMiner event for transaction {}; keeping it in cache.", transactionId, e);
            metrics.incrementWarningCount();
            return TerminalTransactionEventLookup.failed();
        }
    }

    private TerminalTransactionEvent parseTerminalTransactionEvent(String line) {
        final String[] parts = line.split("\\|", 6);
        if (parts.length < 6 || !"@TX".equals(parts[0])) {
            throw new DebeziumException("Bad DBMS_OUTPUT terminal LogMiner event: " + logPreview(line));
        }
        return new TerminalTransactionEvent(
                EventType.from(parseInt(parts[1])),
                Scn.valueOf(parts[2]),
                parseTimestamp(parts[3]),
                nullIfEmpty(parts[4]),
                parseInt(parts[5]));
    }

    protected LogMinerEventRow createTerminalEventRow(String transactionId, TerminalTransactionEvent event) {
        return LogMinerEventRow.fromValues(getConfig().getCatalogName(), event.getScn(), null, event.getEventType().getValue(),
                event.getChangeTime(), transactionId, null, null, event.getEventType().name(), event.getUserName(), null,
                false, null, 0, null, 0, event.getThread());
    }

    protected Scn maxScn(Scn first, Scn second) {
        if (first == null || first.isNull()) {
            return second;
        }
        if (second == null || second.isNull()) {
            return first;
        }
        return first.compareTo(second) >= 0 ? first : second;
    }

    private boolean isHexTransactionId(String transactionId) {
        if (transactionId == null || transactionId.isEmpty()) {
            return false;
        }
        for (int i = 0; i < transactionId.length(); i++) {
            final char c = transactionId.charAt(i);
            if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F'))) {
                return false;
            }
        }
        return true;
    }

    /**
     * Calculates the new starting system change number based on the current processing range.
     *
     * @param endScn the end system change number for the previously mined range, never {@code null}
     * @param maxCommittedScn the maximum committed system change number, never {@code null}
     * @return the system change number to start then next mining iteration from, never {@code null}
     * @throws InterruptedException if the current thread is interrupted
     */
    protected abstract Scn calculateNewStartScn(Scn endScn, Scn maxCommittedScn) throws InterruptedException;

    /**
     * Processes the LogMiner results.
     *
     * @param resultSet the result set from a LogMiner query
     * @throws SQLException if a database exception occurred
     * @throws InterruptedException if the dispatcher was interrupted sending an event
     */
    protected void processResults(OraclePartition partition, ResultSet resultSet) throws SQLException, InterruptedException {
        while (context.isRunning() && hasNextWithMetricsUpdate(resultSet)) {
            counters.rows++;
            processRow(partition, LogMinerEventRow.fromResultSet(resultSet, getConfig().getCatalogName(), isTrxIdRawValue()));
        }
    }

    /**
     * Processes a single LogMinerEventRow.
     *
     * @param row the event row, must not be {@code null}
     * @throws SQLException if a database exception occurred
     * @throws InterruptedException if the dispatcher was interrupted sending an event
     */
    protected void processRow(OraclePartition partition, LogMinerEventRow row) throws SQLException, InterruptedException {
        if (!row.getEventType().equals(EventType.MISSING_SCN)) {
            lastProcessedScn = row.getScn();
        }
        // filter out all events that are captured as part of the initial snapshot
        if (row.getScn().compareTo(offsetContext.getSnapshotScn()) < 0) {
            Map<String, Scn> snapshotPendingTransactions = offsetContext.getSnapshotPendingTransactions();
            if (snapshotPendingTransactions == null || !snapshotPendingTransactions.containsKey(row.getTransactionId())) {
                LOGGER.debug("Skipping event {} (SCN {}) because it is already encompassed by the initial snapshot", row.getEventType(), row.getScn());
                return;
            }
        }
        switch (row.getEventType()) {
            case MISSING_SCN:
                handleMissingScn(row);
            case START:
                handleStart(row);
                break;
            case COMMIT:
                handleCommit(partition, row);
                break;
            case ROLLBACK:
                handleRollback(row);
                break;
            case DDL:
                handleSchemaChange(row);
                break;
            case SELECT_LOB_LOCATOR:
                handleSelectLobLocator(row);
                break;
            case LOB_WRITE:
                handleLobWrite(row);
                break;
            case LOB_ERASE:
                handleLobErase(row);
                break;
            case INSERT:
            case UPDATE:
            case DELETE:
                handleDataEvent(row);
                break;
            case UNSUPPORTED:
                handleUnsupportedEvent(row);
                break;
        }
    }

    /**
     * Handle processing a LogMinerEventRow for a {@code MISSING_SCN} event.
     *
     * @param row the result set row
     */
    protected void handleMissingScn(LogMinerEventRow row) {
        LOGGER.warn("Missing SCN detected. {}", row);
    }

    /**
     * Handle processing a LogMinerEventRow for a {@code START} event.
     *
     * @param row the result set row
     */
    protected void handleStart(LogMinerEventRow row) {
        final String transactionId = row.getTransactionId();
        final AbstractTransaction transaction = getTransactionCache().get(transactionId);
        if (transaction == null && OracleConnectorConfig.LogMiningStrategy.PLSQL_OUTPUT.equals(getConfig().getLogMiningStrategy())) {
            LOGGER.trace("PL/SQL output LogMiner transaction {} START ignored until a captured DML event is seen.", transactionId);
            return;
        }
        if (transaction == null && !isRecentlyProcessed(transactionId)) {
            getTransactionCache().put(transactionId, createTransaction(row));
            metrics.setActiveTransactions(getTransactionCache().size());
        }
        else if (transaction != null && !isRecentlyProcessed(transactionId)) {
            LOGGER.trace("Transaction {} is not yet committed and START event detected.", transactionId);
            transaction.start();
        }
    }

    /**
     * Handle processing a LogMinerEventRow for a {@code COMMIT} event.
     *
     * @param row the result set row
     * @throws InterruptedException if the event dispatcher was interrupted sending events
     */
    protected void handleCommit(OraclePartition partition, LogMinerEventRow row) throws InterruptedException {
        final String transactionId = row.getTransactionId();
        if (isRecentlyProcessed(transactionId)) {
            LOGGER.debug("\tTransaction is already committed, skipped.");
            return;
        }

        final T transaction = getAndRemoveTransactionFromCache(transactionId);
        if (transaction == null) {
            LOGGER.trace("Transaction {} not found, commit skipped.", transactionId);
            return;
        }

        // Calculate the smallest SCN that remains in the transaction cache
        final Scn smallestScn = getTransactionCacheMinimumScn();
        metrics.setOldestScn(smallestScn.isNull() ? Scn.valueOf(-1) : smallestScn);

        final Scn commitScn = row.getScn();
        if (offsetContext.getCommitScn().hasCommitAlreadyBeenHandled(row)) {
            final Scn lastCommittedScn = offsetContext.getCommitScn().getCommitScnForRedoThread(row.getThread());
            LOGGER.debug("Transaction {} has already been processed. "
                    + "Offset Commit SCN {}, Transaction Commit SCN {}, Last Seen Commit SCN {}.",
                    transactionId, offsetContext.getCommitScn(), commitScn, lastCommittedScn);
            removeTransactionAndEventsFromCache(transaction);
            metrics.setActiveTransactions(getTransactionCache().size());
            return;
        }

        counters.commitCount++;

        int numEvents = getTransactionEventCount(transaction);
        LOGGER.trace("Commit (smallest SCN {}) {}", smallestScn, row);
        LOGGER.trace("Transaction {} has {} events", transactionId, numEvents);
        final boolean skipExcludedUserName = isTransactionUserExcluded(transaction, row);
        if (LOGGER.isDebugEnabled() && numEvents > 0
                && OracleConnectorConfig.LogMiningStrategy.PLSQL_OUTPUT.equals(getConfig().getLogMiningStrategy())) {
            LOGGER.debug(
                    "PL/SQL output LogMiner committing transaction: transactionId={}, commitScn={}, events={}, smallestCachedScn={}, transactionUser={}, commitUser={}, excludedUser={}, activeTransactions={}",
                    transactionId, commitScn, numEvents, smallestScn, transaction.getUserName(), row.getUserName(), skipExcludedUserName,
                    getTransactionCache().size());
        }

        final ZoneOffset databaseOffset = metrics.getDatabaseOffset();

        TransactionCommitConsumer.Handler<LogMinerEvent> delegate = new TransactionCommitConsumer.Handler<LogMinerEvent>() {
            private int numEvents = getTransactionEventCount(transaction);

            @Override
            public void accept(LogMinerEvent event, long eventsProcessed) throws InterruptedException {
                // Update SCN in offset context only if processed SCN less than SCN of other transactions
                if (smallestScn.isNull() || commitScn.compareTo(smallestScn) < 0) {
                    offsetContext.setScn(event.getScn());
                    metrics.setOldestScn(event.getScn());
                }

                offsetContext.setEventScn(event.getScn());
                offsetContext.setTransactionId(transactionId);
                offsetContext.setSourceTime(event.getChangeTime().minusSeconds(databaseOffset.getTotalSeconds()));
                offsetContext.setTableId(event.getTableId());
                offsetContext.setRedoThread(row.getThread());
                if (eventsProcessed == numEvents) {
                    // reached the last event update the commit scn in the offsets
                    offsetContext.getCommitScn().recordCommit(row);
                }

                final DmlEvent dmlEvent = (DmlEvent) event;
                if (!skipExcludedUserName) {
                    LogMinerChangeRecordEmitter logMinerChangeRecordEmitter;
                    if (dmlEvent instanceof TruncateEvent) {
                        // a truncate event is seen by logminer as a DDL event type.
                        // So force this here to be a Truncate Operation.
                        logMinerChangeRecordEmitter = new LogMinerChangeRecordEmitter(
                                connectorConfig,
                                partition,
                                offsetContext,
                                Envelope.Operation.TRUNCATE,
                                dmlEvent.getDmlEntry().getOldValues(),
                                dmlEvent.getDmlEntry().getNewValues(),
                                getSchema().tableFor(event.getTableId()),
                                getSchema(),
                                Clock.system());
                    }
                    else {
                        logMinerChangeRecordEmitter = new LogMinerChangeRecordEmitter(
                                connectorConfig,
                                partition,
                                offsetContext,
                                dmlEvent.getEventType(),
                                dmlEvent.getDmlEntry().getOldValues(),
                                dmlEvent.getDmlEntry().getNewValues(),
                                getSchema().tableFor(event.getTableId()),
                                getSchema(),
                                Clock.system());
                    }
                    dispatcher.dispatchDataChangeEvent(partition, event.getTableId(), logMinerChangeRecordEmitter);

                }
            }
        };

        Instant start = Instant.now();
        int dispatchedEventCount = 0;
        if (numEvents > 0) {
            try (TransactionCommitConsumer commitConsumer = new TransactionCommitConsumer(delegate, connectorConfig, schema)) {
                final Iterator<LogMinerEvent> iterator = getTransactionEventIterator(transaction);
                while (iterator.hasNext()) {
                    if (!context.isRunning()) {
                        return;
                    }

                    final LogMinerEvent event = iterator.next();
                    LOGGER.trace("Dispatching event {} {}", ++dispatchedEventCount, event.getEventType());
                    commitConsumer.accept(event);
                }
            }
        }

        offsetContext.setEventScn(commitScn);
        if (getTransactionEventCount(transaction) > 0 && !skipExcludedUserName) {
            dispatcher.dispatchTransactionCommittedEvent(partition, offsetContext);
        }
        else {
            dispatcher.dispatchHeartbeatEvent(partition, offsetContext);
        }

        metrics.calculateLagMetrics(row.getChangeTime());

        finalizeTransactionCommit(transactionId, commitScn);
        removeTransactionAndEventsFromCache(transaction);

        metrics.incrementCommittedTransactions();
        metrics.setActiveTransactions(getTransactionCache().size());
        metrics.incrementCommittedDmlCount(dispatchedEventCount);
        metrics.setCommittedScn(commitScn);
        metrics.setOffsetScn(offsetContext.getScn());
        metrics.setLastCommitDuration(Duration.between(start, Instant.now()));
    }

    /**
     * Gets a transaction instance from the transaction cache while also removing its cache entry.
     *
     * @param transactionId the transaction's unique identifier, should not be {@code null}
     * @return the transaction instance if found, {@code null} if the transaction wasn't found
     */
    protected abstract T getAndRemoveTransactionFromCache(String transactionId);

    /**
     * Removes the transaction and all its associated event entries from the connector's caches.
     *
     * @param transaction the transaction instance, should never be {@code null}
     */
    protected abstract void removeTransactionAndEventsFromCache(T transaction);

    /**
     * Get an iterator over the events that are part of the specified transaction.
     *
     * @param transaction the transaction instance, should never be {@code null}
     * @return an iterator over the transaction's events, never {@code null}
     */
    protected abstract Iterator<LogMinerEvent> getTransactionEventIterator(T transaction);

    /**
     * Finalizes the commit of a transaction.
     *
     * @param transactionId the transaction's unique identifier, should not be {@code null}
     * @param commitScn the transaction's system change number, should not be {@code null}
     */
    protected abstract void finalizeTransactionCommit(String transactionId, Scn commitScn);

    /**
     * Check whether the username associated with the transaction or its commit row is excluded.
     *
     * @param transaction the transaction, may be {@code null}
     * @param commitRow the commit event row, may be {@code null}
     * @return true if the transaction should be skipped; false if transaction should be emitted
     */
    protected boolean isTransactionUserExcluded(T transaction, LogMinerEventRow commitRow) {
        if (transaction != null) {
            if (transaction.getUserName() == null && getTransactionEventCount(transaction) > 0) {
                LOGGER.debug("Detected transaction with null username {}", transaction);
            }
            else if (connectorConfig.getLogMiningUsernameExcludes().contains(transaction.getUserName())) {
                LOGGER.trace("Skipped transaction with excluded username {}", transaction);
                return true;
            }
        }
        if (commitRow != null && connectorConfig.getLogMiningUsernameExcludes().contains(commitRow.getUserName())) {
            LOGGER.trace("Skipped transaction {} with excluded commit username {}", commitRow.getTransactionId(), commitRow.getUserName());
            return true;
        }
        return false;
    }

    /**
     * Handle processing a LogMinerEventRow for a {@code ROLLBACK} event.
     *
     * @param row the result set row
     */
    protected void handleRollback(LogMinerEventRow row) {
        if (getTransactionCache().containsKey(row.getTransactionId())) {
            LOGGER.trace("Transaction {} was rolled back.", row.getTransactionId());
            finalizeTransactionRollback(row.getTransactionId(), row.getScn());
            metrics.setActiveTransactions(getTransactionCache().size());
            metrics.incrementRolledBackTransactions();
            metrics.addRolledBackTransactionId(row.getTransactionId());
            counters.rollbackCount++;
        }
        else {
            LOGGER.trace("Could not rollback transaction {}, was not found in cache.", row.getTransactionId());
        }
    }

    /**
     * Finalizes the rollback the specified transaction
     *
     * @param transactionId the unique transaction identifier, never {@code null}
     * @param rollbackScn the rollback transaction's system change number, never {@code null}
     */
    protected abstract void finalizeTransactionRollback(String transactionId, Scn rollbackScn);

    /**
     * Handle processing a LogMinerEventRow for a {@code DDL} event.
     *
     * @param row the result set row
     * @throws InterruptedException if the event dispatcher is interrupted sending the event
     */
    protected void handleSchemaChange(LogMinerEventRow row) throws InterruptedException {
        if (hasSchemaChangeBeenSeen(row)) {
            LOGGER.trace("DDL: Scn {}, SQL '{}' has already been processed, skipped.", row.getScn(), row.getRedoSql());
            return;
        }

        if (offsetContext.getCommitScn().hasCommitAlreadyBeenHandled(row)) {
            final Scn commitScn = offsetContext.getCommitScn().getCommitScnForRedoThread(row.getThread());
            LOGGER.trace("DDL: SQL '{}' skipped with {} (SCN) <= {} (commit SCN for redo thread {})",
                    row.getRedoSql(), row.getScn(), commitScn, row.getThread());
            return;
        }

        LOGGER.trace("DDL: '{}' {}", row.getRedoSql(), row);
        if (row.getTableName() != null) {
            counters.ddlCount++;
            final TableId tableId = row.getTableId();

            final int activeTransactions = getTransactionCache().size();
            boolean advanceLowerScnBoundary = false;
            if (activeTransactions == 0) {
                // The DDL isn't wrapped in a transaction, fast-forward the lower boundary
                advanceLowerScnBoundary = true;
            }
            else if (activeTransactions == 1) {
                final String transactionId = getTransactionCache().keySet().iterator().next();
                if (transactionId.equals(row.getTransactionId())) {
                    // The row's transaction is the current and only active transaction.
                    advanceLowerScnBoundary = true;
                }
            }

            if (advanceLowerScnBoundary) {
                LOGGER.debug("Schema change advanced offset SCN to {}", row.getScn());
                offsetContext.setScn(row.getScn());
            }

            // Should always advance the commit SCN point with schema changes
            LOGGER.debug("Schema change advanced offset commit SCN to {} for thread {}", row.getScn(), row.getThread());
            offsetContext.getCommitScn().recordCommit(row);

            offsetContext.setEventScn(row.getScn());
            offsetContext.setRedoThread(row.getThread());
            dispatcher.dispatchSchemaChangeEvent(partition,
                    tableId,
                    new OracleSchemaChangeEventEmitter(
                            getConfig(),
                            partition,
                            offsetContext,
                            tableId,
                            tableId.catalog(),
                            tableId.schema(),
                            row.getRedoSql(),
                            getSchema(),
                            row.getChangeTime(),
                            metrics,
                            () -> processTruncateEvent(row)));
        }
    }

    private void processTruncateEvent(LogMinerEventRow row) {
        LOGGER.debug("Handling truncate event");
        addToTransaction(row.getTransactionId(), row, () -> {
            final LogMinerDmlEntry dmlEntry = LogMinerDmlEntryImpl.forValuelessDdl();
            dmlEntry.setObjectName(row.getTableName());
            dmlEntry.setObjectOwner(row.getTablespaceName());
            return new TruncateEvent(row, dmlEntry);
        });
    }

    /**
     * Handle processing a LogMinerEventRow for a {@code SEL_LOB_LOCATOR} event.
     *
     * @param row the result set row
     */
    protected void handleSelectLobLocator(LogMinerEventRow row) {
        if (!getConfig().isLobEnabled()) {
            LOGGER.trace("LOB support is disabled, SEL_LOB_LOCATOR '{}' skipped.", row.getRedoSql());
            return;
        }

        LOGGER.trace("SEL_LOB_LOCATOR: {}", row);
        final TableId tableId = row.getTableId();
        final Table table = getSchema().tableFor(tableId);
        if (table == null) {
            LOGGER.warn("SEL_LOB_LOCATOR for table '{}' is not known, skipped.", tableId);
            return;
        }

        addToTransaction(row.getTransactionId(),
                row,
                () -> {
                    final LogMinerDmlEntry dmlEntry = selectLobParser.parse(row.getRedoSql(), table);
                    dmlEntry.setObjectName(row.getTableName());
                    dmlEntry.setObjectOwner(row.getTablespaceName());

                    return new SelectLobLocatorEvent(row,
                            dmlEntry,
                            selectLobParser.getColumnName(),
                            selectLobParser.isBinary());
                });

        metrics.incrementRegisteredDmlCount();
    }

    /**
     * Handle processing a LogMinerEventRow for a {@code LOB_WRITE} event.
     *
     * @param row the result set row
     */
    protected void handleLobWrite(LogMinerEventRow row) {
        if (!getConfig().isLobEnabled()) {
            LOGGER.trace("LOB support is disabled, LOB_WRITE scn={}, tableId={} skipped", row.getScn(), row.getTableId());
            return;
        }

        LOGGER.trace("LOB_WRITE: scn={}, tableId={}, changeTime={}, transactionId={}",
                row.getScn(), row.getTableId(), row.getChangeTime(), row.getTransactionId());

        final TableId tableId = row.getTableId();
        final Table table = getSchema().tableFor(tableId);
        if (table == null) {
            LOGGER.warn("LOB_WRITE for table '{}' is not known, skipped", tableId);
            return;
        }

        if (row.getRedoSql() != null) {
            addToTransaction(row.getTransactionId(), row, () -> {
                final ParsedLobWriteSql parsed = parseLobWriteSql(row.getRedoSql());
                return new LobWriteEvent(row, parsed.data, parsed.offset, parsed.length);
            });
        }
    }

    /**
     * Handle processing a LogMinerEventRow for a {@code LOB_ERASE} event.
     *
     * @param row the result set row
     */
    private void handleLobErase(LogMinerEventRow row) {
        if (!getConfig().isLobEnabled()) {
            LOGGER.trace("LOB support is disabled, LOB_ERASE '{}' skipped", row);
            return;
        }

        LOGGER.trace("LOB_ERASE: {}", row);
        final TableId tableId = row.getTableId();
        final Table table = getSchema().tableFor(tableId);
        if (table == null) {
            LOGGER.warn("LOB_ERASE for table '{}' is not known, skipped", tableId);
            return;
        }

        addToTransaction(row.getTransactionId(), row, () -> new LobEraseEvent(row));
    }

    /**
     * Handle processing a LogMinerEventRow for a {@code INSERT}, {@code UPDATE}, or {@code DELETE} event.
     *
     * @param row the result set row
     * @throws SQLException if a database exception occurs
     * @throws InterruptedException if the dispatch of an event is interrupted
     */
    protected void handleDataEvent(LogMinerEventRow row) throws SQLException, InterruptedException {
        if (row.getRedoSql() == null) {
            return;
        }

        LOGGER.trace("DML: {}", row);
        LOGGER.trace("\t{}", row.getRedoSql());

        // Oracle LogMiner reports LONG data types as STATUS=2 on UPDATE statements but there is no
        // value in the INFO column, and the record can be managed by the connector successfully,
        // so to be backward compatible, we only explicitly trigger this behavior if there is an
        // error reason for STATUS=2 in the INFO column as well as STATUS=2.
        if (row.getStatus() == 2 && !Strings.isNullOrBlank(row.getInfo())) {
            // The SQL in the SQL_REDO column is not valid and cannot be parsed.
            switch (connectorConfig.getEventProcessingFailureHandlingMode()) {
                case FAIL:
                    LOGGER.error("Oracle LogMiner is unable to re-construct the SQL for '{}'", row);
                    throw new DebeziumException("Oracle failed to re-construct redo SQL '" + row.getRedoSql() + "'");
                case WARN:
                    LOGGER.warn("Oracle LogMiner event '{}' cannot be parsed. This event will be ignored and skipped.", row);
                    return;
                default:
                    // In this case, we explicitly log the situation in "debug" only and not as an error/warn.
                    LOGGER.debug("Oracle LogMiner event '{}' cannot be parsed. This event will be ignored and skipped.", row);
                    return;
            }
        }

        counters.dmlCount++;
        switch (row.getEventType()) {
            case INSERT:
                counters.insertCount++;
                break;
            case UPDATE:
                counters.updateCount++;
                break;
            case DELETE:
                counters.deleteCount++;
                break;
        }

        final Table table = getTableForDataEvent(row);
        if (table == null) {
            return;
        }

        if (row.isRollbackFlag()) {
            // There is a use case where a constraint violation will result in a DML event being
            // written to the redo log subsequently followed by another DML event that is marked
            // with a rollback flag to indicate that the prior event should be omitted. In this
            // use case, the transaction can still be committed, so we need to manually rollback
            // the previous DML event when this use case occurs.
            removeEventWithRowId(row);
            return;
        }

        addToTransaction(row.getTransactionId(), row, () -> {
            final LogMinerDmlEntry dmlEntry = parseDmlStatement(row.getRedoSql(), table, row.getTransactionId());
            dmlEntry.setObjectName(row.getTableName());
            dmlEntry.setObjectOwner(row.getTablespaceName());
            return new DmlEvent(row, dmlEntry);
        });

        metrics.incrementRegisteredDmlCount();
    }

    protected void handleUnsupportedEvent(LogMinerEventRow row) {
        if (!Strings.isNullOrEmpty(row.getTableName())) {
            LOGGER.warn("An unsupported operation detected for table '{}' in transaction {} with SCN {} on redo thread {}.",
                    row.getTableId(),
                    row.getTransactionId(),
                    row.getScn(),
                    row.getThread());
        }
    }

    /**
     * Checks to see whether the offset's {@code scn} is remaining the same across multiple mining sessions
     * while the offset's {@code commit_scn} is changing between sessions.
     *
     * @param previousOffsetScn the previous offset system change number
     * @param previousOffsetCommitScns the previous offset commit system change number
     */
    protected void warnPotentiallyStuckScn(Scn previousOffsetScn, Map<Integer, Scn> previousOffsetCommitScns) {
        if (offsetContext != null && offsetContext.getCommitScn() != null) {
            final Scn scn = offsetContext.getScn();
            final Map<Integer, Scn> commitScns = offsetContext.getCommitScn().getCommitScnForAllRedoThreads();
            if (previousOffsetScn.equals(scn) && !previousOffsetCommitScns.equals(commitScns)) {
                counters.stuckCount++;
                if (counters.stuckCount == 25) {
                    LOGGER.warn("Offset SCN {} has not changed in 25 mining session iterations. " +
                            "This indicates long running transaction(s) are active.  Commit SCNs {}.",
                            previousOffsetScn,
                            previousOffsetCommitScns);
                    metrics.incrementScnFreezeCount();
                }
            }
            else {
                counters.stuckCount = 0;
            }
        }
    }

    private Table getTableForDataEvent(LogMinerEventRow row) throws SQLException, InterruptedException {
        final TableId tableId = row.getTableId();
        Table table = getSchema().tableFor(tableId);
        if (table == null) {
            if (!getConfig().getTableFilters().dataCollectionFilter().isIncluded(tableId)) {
                if (OracleConnectorConfig.LogMiningStrategy.PLSQL_OUTPUT.equals(getConfig().getLogMiningStrategy())) {
                    LOGGER.info(
                            "PL/SQL output LogMiner data event skipped because table filter does not include tableId={}: scn={}, operation={}, transactionId={}, sqlRedo={}",
                            tableId, row.getScn(), row.getEventType(), row.getTransactionId(), logPreview(row.getRedoSql()));
                }
                return null;
            }
            table = dispatchSchemaChangeEventAndGetTableForNewCapturedTable(tableId, offsetContext, dispatcher);
        }
        return table;
    }

    /**
     * Checks whether the result-set has any more data available.
     * When a new row is available, the streaming metrics is updated with the fetch timings.
     *
     * @param resultSet the result set to check if any more rows exist
     * @return true if another row exists, false otherwise
     * @throws SQLException if there was a database exception
     */
    private boolean hasNextWithMetricsUpdate(ResultSet resultSet) throws SQLException {
        Instant start = Instant.now();
        boolean result = false;
        try {
            if (resultSet.next()) {
                metrics.addCurrentResultSetNext(Duration.between(start, Instant.now()));
                result = true;
            }

            // Reset sequence unavailability on successful read from the result set
            if (sequenceUnavailable) {
                LOGGER.debug("The previous batch's unavailable log problem has been cleared.");
                sequenceUnavailable = false;
            }
        }
        catch (SQLException e) {
            // Oracle's online redo logs can be defined with dynamic names using the instance
            // configuration property LOG_ARCHIVE_FORMAT.
            //
            // Dynamically named online redo logs can lead to ORA-00310 errors if a log switch
            // happens while the processor is iterating the LogMiner session's result set and
            // LogMiner can no longer read the next batch of records from the log.
            //
            // LogMiner only validates that there are no gaps and that the logs are available
            // when the session is first started and any change in the logs later will raise
            // these types of errors.
            //
            // Catching the ORA-00310 and treating it as the end of the result set will allow
            // the connector's outer loop to re-evaluate the log state and start a new LogMiner
            // session with the new logs. The connector will then begin streaming from where
            // it left off. If any other exception is caught here, it'll be thrown.
            if (!e.getMessage().startsWith("ORA-00310")) {
                // throw any non ORA-00310 error, old behavior
                throw e;
            }
            else if (sequenceUnavailable) {
                // If an ORA-00310 error was raised on the previous iteration and wasn't cleared
                // after re-evaluation of the log availability and the mining session, we will
                // explicitly stop the connector to avoid an infinite loop.
                LOGGER.error("The log availability error '{}' wasn't cleared, stop requested.", e.getMessage());
                throw e;
            }

            LOGGER.debug("A mined log is no longer available: {}", e.getMessage());
            LOGGER.warn("Restarting mining session after a log became unavailable.");

            // Track that we gracefully stopped due to a ORA-00310.
            // Will be used to detect an infinite loop of this error across sequential iterations
            sequenceUnavailable = true;
        }
        return result;
    }

    /**
     * Add a transaction to the transaction map if allowed.
     *
     * @param transactionId the unqiue transaction id
     * @param row the LogMiner event row
     * @param eventSupplier the supplier of the event to create if the event is allowed to be added
     */
    protected abstract void addToTransaction(String transactionId, LogMinerEventRow row, Supplier<LogMinerEvent> eventSupplier);

    /**
     * Dispatch a schema change event for a new table and get the newly created relational table model.
     *
     * @param tableId the unique table identifier, must not be {@code null}
     * @param offsetContext the offset context
     * @param dispatcher the event dispatcher
     * @return the relational table model
     * @throws SQLException if a database exception occurred
     * @throws InterruptedException if the event dispatch was interrupted
     */
    private Table dispatchSchemaChangeEventAndGetTableForNewCapturedTable(TableId tableId,
                                                                          OracleOffsetContext offsetContext,
                                                                          EventDispatcher<OraclePartition, TableId> dispatcher)
            throws SQLException, InterruptedException {

        final String tableDdl;
        try {
            tableDdl = getTableMetadataDdl(tableId);
        }
        catch (NonRelationalTableException e) {
            LOGGER.warn("Table {} is not a relational table and will be skipped.", tableId);
            metrics.incrementWarningCount();
            return null;
        }

        LOGGER.info("Table '{}' is new and will now be captured.", tableId);
        offsetContext.event(tableId, Instant.now());
        dispatcher.dispatchSchemaChangeEvent(partition,
                tableId,
                new OracleSchemaChangeEventEmitter(connectorConfig,
                        partition,
                        offsetContext,
                        tableId,
                        tableId.catalog(),
                        tableId.schema(),
                        tableDdl,
                        getSchema(),
                        Instant.now(),
                        metrics,
                        null));

        return getSchema().tableFor(tableId);
    }

    /**
     * Get the specified table's create DDL statement.
     *
     * @param tableId the table identifier, must not be {@code null}
     * @return the table's create DDL statement, never {@code null}
     * @throws SQLException if an exception occurred obtaining the DDL statement
     * @throws NonRelationalTableException if the table is not a relational table
     */
    private String getTableMetadataDdl(TableId tableId) throws SQLException, NonRelationalTableException {
        counters.tableMetadataCount++;
        LOGGER.info("Getting database metadata for table '{}'", tableId);
        // A separate connection must be used for this out-of-bands query while processing LogMiner results.
        // This should have negligible overhead since this use case should happen rarely.
        try (OracleConnection connection = new OracleConnection(connectorConfig.getJdbcConfig(), () -> getClass().getClassLoader(), false)) {
            connection.setAutoCommit(false);
            final String pdbName = getConfig().getPdbName();
            if (pdbName != null) {
                connection.setSessionToPdb(pdbName);
            }
            return connection.getTableMetadataDdl(tableId);
        }
    }

    /**
     * Parse a DML redo SQL statement.
     *
     * @param redoSql the redo SQL statement
     * @param table the table the SQL statement is for
     * @param transactionId the associated transaction id for the SQL statement
     * @return a parse object for the redo SQL statement
     */
    private LogMinerDmlEntry parseDmlStatement(String redoSql, Table table, String transactionId) {
        LogMinerDmlEntry dmlEntry;
        try {
            Instant parseStart = Instant.now();
            dmlEntry = dmlParser.parse(redoSql, table);
            metrics.addCurrentParseTime(Duration.between(parseStart, Instant.now()));
        }
        catch (DmlParserException e) {
            String message = "DML statement couldn't be parsed." +
                    " Please open a Jira issue with the statement '" + redoSql + "'.";
            throw new DmlParserException(message, e);
        }

        if (dmlEntry.getOldValues().length == 0) {
            if (EventType.UPDATE == dmlEntry.getEventType() || EventType.DELETE == dmlEntry.getEventType()) {
                LOGGER.warn("The DML event '{}' contained no before state.", redoSql);
                metrics.incrementWarningCount();
            }
        }

        return dmlEntry;
    }

    private static Pattern LOB_WRITE_SQL_PATTERN = Pattern.compile(
            "(?s).* := ((?:HEXTORAW\\()?'.*'(?:\\))?);\\s*dbms_lob.write\\([^,]+,\\s*(\\d+)\\s*,\\s*(\\d+)\\s*,[^,]+\\);.*");

    /**
     * Parses a {@code LOB_WRITE} operation SQL fragment.
     *
     * @param sql sql statement
     * @return the parsed statement
     * @throws DebeziumException if an unexpected SQL fragment is provided that cannot be parsed
     */
    private ParsedLobWriteSql parseLobWriteSql(String sql) {
        if (sql == null) {
            return null;
        }

        Matcher m = LOB_WRITE_SQL_PATTERN.matcher(sql.trim());
        if (!m.matches()) {
            throw new DebeziumException("Unable to parse unsupported LOB_WRITE SQL: " + sql);
        }

        String data = m.group(1);
        if (data.startsWith("'")) {
            // string data; drop the quotes
            data = data.substring(1, data.length() - 1);
        }
        int length = Integer.parseInt(m.group(2));
        int offset = Integer.parseInt(m.group(3)) - 1; // Oracle uses 1-based offsets
        return new ParsedLobWriteSql(offset, length, data);
    }

    private class ParsedLobWriteSql {
        final int offset;
        final int length;
        final String data;

        ParsedLobWriteSql(int _offset, int _length, String _data) {
            offset = _offset;
            length = _length;
            data = _data;
        }
    }

    /**
     * Gets the minimum system change number stored in the transaction cache.
     * @return the minimum system change number, never {@code null} but could be {@link Scn#NULL}.
     */
    protected abstract Scn getTransactionCacheMinimumScn();

    /**
     * Returns whether the transaction id has no sequence number component.
     *
     * Oracle transaction identifiers are a composite of:
     * <ol>
     *     <li>Undo segment number</li>
     *     <li>Slot numbber of the transaction that generated the change</li>
     *     <li>Sequence number of the transaction that generated the change</li>
     * </ol>
     *
     * When Oracle LogMiner mines records, it is possible that when an undo operation is detected,
     * often the product of a constraint violation, the LogMiner row will have the same explicit
     * XID (transaction id) as the source operation that we should undo; however, if the record
     * to be undone was mined in a prior iteration, Oracle LogMiner won't be able to make a link
     * back to the full transaction's sequence number, therefore the XID value for the undo row
     * will contain only the undo segment number and slot number, setting the sequence number to
     * 4294967295 (aka -1 or 0xFFFFFFFF).
     *
     * This method explicitly checks if the provided transaction id has the no sequence sentinel
     * value and if so, returns {@code true}; otherwise returns {@code false}.
     *
     * @param transactionId the transaction identifier to check, should not be {@code null}
     * @return true if the transaction has no sequence reference, false if it does
     */
    protected boolean isTransactionIdWithNoSequence(String transactionId) {
        return transactionId.endsWith(NO_SEQUENCE_TRX_ID_SUFFIX);
    }

    protected String getTransactionIdPrefix(String transactionId) {
        return transactionId.substring(0, 8);
    }

    private static class DbmsOutputLine {
        private final String value;
        private final int status;

        private DbmsOutputLine(String value, int status) {
            this.value = value;
            this.status = status;
        }
    }

    private static class DbmsOutputLineReader implements AutoCloseable {
        private final OracleCallableStatement batchStatement;
        private final CallableStatement singleLineStatement;
        private final String[] lines = new String[PLSQL_OUTPUT_GET_LINES_BATCH_SIZE];
        private int index;
        private int count;
        private int lineLengthIndex;
        private long fetchCalls;
        private long fetchedLines;
        private boolean exhausted;
        private boolean singleLineMode;
        private boolean fallbackLogged;

        private DbmsOutputLineReader(PreparedStatement queryStatement) throws SQLException {
            CallableStatement callableStatement = queryStatement.getConnection()
                    .prepareCall("BEGIN DBMS_OUTPUT.GET_LINES(?, ?); END;");
            this.batchStatement = callableStatement.unwrap(OracleCallableStatement.class);
            this.singleLineStatement = queryStatement.getConnection().prepareCall("BEGIN DBMS_OUTPUT.GET_LINE(?, ?); END;");
        }

        private DbmsOutputLine readLine() throws SQLException {
            if (singleLineMode) {
                return readSingleLine();
            }
            if (index >= count && !fetch()) {
                return new DbmsOutputLine(null, 1);
            }
            return new DbmsOutputLine(lines[index++], 0);
        }

        private boolean fetch() throws SQLException {
            if (singleLineMode) {
                return false;
            }
            if (exhausted) {
                return false;
            }
            Arrays.fill(lines, null);
            while (true) {
                try {
                    batchStatement.registerIndexTableOutParameter(1, PLSQL_OUTPUT_GET_LINES_BATCH_SIZE,
                            OracleTypes.VARCHAR, PLSQL_OUTPUT_GET_LINES_LINE_LENGTHS[lineLengthIndex]);
                    break;
                }
                catch (SQLException e) {
                    if (++lineLengthIndex >= PLSQL_OUTPUT_GET_LINES_LINE_LENGTHS.length) {
                        singleLineMode = true;
                        if (!fallbackLogged) {
                            LOGGER.warn(
                                    "PL/SQL output LogMiner DBMS_OUTPUT.GET_LINES is not supported by the Oracle JDBC driver; falling back to DBMS_OUTPUT.GET_LINE. Last error: {}",
                                    e.getMessage());
                            fallbackLogged = true;
                        }
                        return readSingleLineIntoBatch();
                    }
                    LOGGER.warn("PL/SQL output LogMiner DBMS_OUTPUT.GET_LINES line size {} rejected by Oracle JDBC driver, retrying with {}. Error: {}",
                            PLSQL_OUTPUT_GET_LINES_LINE_LENGTHS[lineLengthIndex - 1],
                            PLSQL_OUTPUT_GET_LINES_LINE_LENGTHS[lineLengthIndex],
                            e.getMessage());
                }
            }
            batchStatement.setInt(2, PLSQL_OUTPUT_GET_LINES_BATCH_SIZE);
            batchStatement.registerOutParameter(2, Types.INTEGER);
            batchStatement.execute();

            Object output = batchStatement.getPlsqlIndexTable(1);
            int fetched = batchStatement.getInt(2);
            fetchCalls++;
            if (fetched <= 0 || output == null) {
                exhausted = true;
                count = 0;
                index = 0;
                return false;
            }

            String[] fetchedLines = (String[]) output;
            count = Math.min(fetched, fetchedLines.length);
            System.arraycopy(fetchedLines, 0, lines, 0, count);
            index = 0;
            this.fetchedLines += count;
            return true;
        }

        private boolean readSingleLineIntoBatch() throws SQLException {
            DbmsOutputLine line = readSingleLine();
            if (line.status != 0) {
                exhausted = true;
                count = 0;
                index = 0;
                return false;
            }
            lines[0] = line.value;
            count = 1;
            index = 0;
            return true;
        }

        private DbmsOutputLine readSingleLine() throws SQLException {
            singleLineStatement.registerOutParameter(1, Types.VARCHAR);
            singleLineStatement.registerOutParameter(2, Types.INTEGER);
            singleLineStatement.execute();
            fetchCalls++;
            int status = singleLineStatement.getInt(2);
            if (status == 0) {
                fetchedLines++;
            }
            return new DbmsOutputLine(singleLineStatement.getString(1), status);
        }

        private long getFetchCalls() {
            return fetchCalls;
        }

        private long getFetchedLines() {
            return fetchedLines;
        }

        @Override
        public void close() throws SQLException {
            SQLException error = null;
            try {
                batchStatement.close();
            }
            catch (SQLException e) {
                error = e;
            }
            try {
                singleLineStatement.close();
            }
            catch (SQLException e) {
                if (error != null) {
                    error.addSuppressed(e);
                }
                else {
                    error = e;
                }
            }
            if (error != null) {
                throw error;
            }
        }
    }

    private static class PlSqlOutputReadStats {
        private long outputLines;
        private long outputFetches;
        private long outputRows;
        private long completedRows;
        private boolean flushedPartialRow;
        private boolean truncated;
        private Scn effectiveEndScn;
        private Scn lastScn = Scn.NULL;
        private int lastOperationCode;
        private String lastOperation;
        private String lastTableName;
        private String lastOwner;
        private String lastTransactionId;
        private int lastCsf;
        private int lastThread;
        private final Map<Integer, Long> operationCounts = new HashMap<>();
        private int sampledDmlRows;
        private int sampledOtherRows;

        private void remember(PlSqlOutputRow row) {
            operationCounts.merge(row.operationCode, 1L, Long::sum);
            lastScn = row.scn;
            lastOperationCode = row.operationCode;
            lastOperation = row.operation;
            lastTableName = row.tableName;
            lastOwner = row.tablespaceName;
            lastTransactionId = row.transactionId;
            lastCsf = row.csf;
            lastThread = row.thread;
        }

        private boolean shouldLogSample(PlSqlOutputRow row) {
            if (row.operationCode == EventType.INSERT.getValue() || row.operationCode == EventType.UPDATE.getValue()
                    || row.operationCode == EventType.DELETE.getValue() || row.operationCode == EventType.UNSUPPORTED.getValue()) {
                return sampledDmlRows++ < PLSQL_OUTPUT_SAMPLE_LIMIT;
            }
            if (row.operationCode != EventType.START.getValue() && row.operationCode != EventType.COMMIT.getValue()
                    && row.operationCode != EventType.ROLLBACK.getValue() && row.operationCode != EventType.MISSING_SCN.getValue()) {
                return sampledOtherRows++ < PLSQL_OUTPUT_SAMPLE_LIMIT;
            }
            return false;
        }

        private String describeOperationCounts() {
            return operationCounts.toString();
        }

        private String describeLastRow() {
            if (outputRows == 0) {
                return "<none>";
            }
            return "scn=" + lastScn +
                    ", operationCode=" + lastOperationCode +
                    ", operation=" + lastOperation +
                    ", owner=" + lastOwner +
                    ", table=" + lastTableName +
                    ", transactionId=" + lastTransactionId +
                    ", csf=" + lastCsf +
                    ", thread=" + lastThread +
                    ", truncated=" + truncated +
                    ", effectiveEndScn=" + effectiveEndScn +
                    ", flushedPartialRow=" + flushedPartialRow;
        }

        private Scn getEffectiveEndScn(Scn configuredEndScn) {
            return effectiveEndScn == null ? configuredEndScn : effectiveEndScn;
        }
    }

    private static class PlSqlOutputRow {
        private final Scn scn;
        private final int operationCode;
        private final Instant changeTime;
        private final String transactionId;
        private final int csf;
        private final String tableName;
        private final String tablespaceName;
        private final String operation;
        private final String userName;
        private final String rowId;
        private final boolean rollbackFlag;
        private final String rsId;
        private final int status;
        private final int ssn;
        private final int thread;
        private final String info;
        private final String sqlRedo;

        private PlSqlOutputRow(Scn scn, int operationCode, Instant changeTime, String transactionId, int csf,
                               String tableName, String tablespaceName, String operation, String userName,
                               String rowId, boolean rollbackFlag, String rsId, int status, int ssn, int thread,
                               String info, String sqlRedo) {
            this.scn = scn;
            this.operationCode = operationCode;
            this.changeTime = changeTime;
            this.transactionId = transactionId;
            this.csf = csf;
            this.tableName = tableName;
            this.tablespaceName = tablespaceName;
            this.operation = operation;
            this.userName = userName;
            this.rowId = rowId;
            this.rollbackFlag = rollbackFlag;
            this.rsId = rsId;
            this.status = status;
            this.ssn = ssn;
            this.thread = thread;
            this.info = info;
            this.sqlRedo = sqlRedo;
        }
    }

    private static class PlSqlOutputRowBuilder {
        private final PlSqlOutputRow row;
        private final StringBuilder sqlRedo = new StringBuilder();

        private PlSqlOutputRowBuilder(PlSqlOutputRow row) {
            this.row = row;
            appendSqlRedo(row.sqlRedo);
        }

        private void appendSqlRedo(String value) {
            if (value != null) {
                sqlRedo.append(value);
            }
        }

        private LogMinerEventRow build(String catalogName) {
            return LogMinerEventRow.fromValues(
                    catalogName,
                    row.scn,
                    sqlRedo.length() == 0 ? null : sqlRedo.toString(),
                    row.operationCode,
                    row.changeTime,
                    row.transactionId,
                    row.tableName,
                    row.tablespaceName,
                    row.operation,
                    row.userName,
                    row.rowId,
                    row.rollbackFlag,
                    row.rsId,
                    row.status,
                    row.info,
                    row.ssn,
                    row.thread);
        }
    }

    protected static class TerminalTransactionEvent {
        private final EventType eventType;
        private final Scn scn;
        private final Instant changeTime;
        private final String userName;
        private final int thread;

        private TerminalTransactionEvent(EventType eventType, Scn scn, Instant changeTime, String userName, int thread) {
            this.eventType = eventType;
            this.scn = scn;
            this.changeTime = changeTime;
            this.userName = userName;
            this.thread = thread;
        }

        public EventType getEventType() {
            return eventType;
        }

        public Scn getScn() {
            return scn;
        }

        public Instant getChangeTime() {
            return changeTime;
        }

        public String getUserName() {
            return userName;
        }

        public int getThread() {
            return thread;
        }
    }

    protected static class TerminalTransactionEventLookup {
        private final TerminalTransactionEvent event;
        private final boolean failed;

        private TerminalTransactionEventLookup(TerminalTransactionEvent event, boolean failed) {
            this.event = event;
            this.failed = failed;
        }

        protected static TerminalTransactionEventLookup found(TerminalTransactionEvent event) {
            return new TerminalTransactionEventLookup(event, false);
        }

        protected static TerminalTransactionEventLookup notFound() {
            return new TerminalTransactionEventLookup(null, false);
        }

        protected static TerminalTransactionEventLookup failed() {
            return new TerminalTransactionEventLookup(null, true);
        }

        public boolean isPresent() {
            return event != null;
        }

        public boolean isFailed() {
            return failed;
        }

        public TerminalTransactionEvent getEvent() {
            return event;
        }
    }

    /**
     * Wrapper for all counter variables
     *
     */
    protected class Counters {
        public int stuckCount;
        public int dmlCount;
        public int ddlCount;
        public int insertCount;
        public int updateCount;
        public int deleteCount;
        public int commitCount;
        public int rollbackCount;
        public int tableMetadataCount;
        public long rows;

        public void reset() {
            stuckCount = 0;
            dmlCount = 0;
            ddlCount = 0;
            insertCount = 0;
            updateCount = 0;
            deleteCount = 0;
            commitCount = 0;
            rollbackCount = 0;
            tableMetadataCount = 0;
            rows = 0;
        }

        @Override
        public String toString() {
            return "Counters{" +
                    "rows=" + rows +
                    ", stuckCount=" + stuckCount +
                    ", dmlCount=" + dmlCount +
                    ", ddlCount=" + ddlCount +
                    ", insertCount=" + insertCount +
                    ", updateCount=" + updateCount +
                    ", deleteCount=" + deleteCount +
                    ", commitCount=" + commitCount +
                    ", rollbackCount=" + rollbackCount +
                    ", tableMetadataCount=" + tableMetadataCount +
                    '}';
        }
    }
}
