/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.processor.memory;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleDatabaseSchema;
import io.debezium.connector.oracle.OracleOffsetContext;
import io.debezium.connector.oracle.OraclePartition;
import io.debezium.connector.oracle.OracleStreamingChangeEventSourceMetrics;
import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.logminer.LogMinerQueryBuilder;
import io.debezium.connector.oracle.logminer.SqlUtils;
import io.debezium.connector.oracle.logminer.events.EventType;
import io.debezium.connector.oracle.logminer.events.LogMinerEvent;
import io.debezium.connector.oracle.logminer.events.LogMinerEventRow;
import io.debezium.connector.oracle.logminer.processor.AbstractLogMinerEventProcessor;
import io.debezium.connector.oracle.logminer.processor.LogMinerEventProcessor;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.ChangeEventSource.ChangeEventSourceContext;
import io.debezium.relational.TableId;

/**
 * A {@link LogMinerEventProcessor} that uses the JVM heap to store events as they're being
 * processed and emitted from Oracle LogMiner.
 *
 * @author Chris Cranford
 */
public class MemoryLogMinerEventProcessor extends AbstractLogMinerEventProcessor<MemoryTransaction> {

    private static final Logger LOGGER = LoggerFactory.getLogger(MemoryLogMinerEventProcessor.class);

    private final OracleConnection jdbcConnection;
    private final EventDispatcher<OraclePartition, TableId> dispatcher;
    private final OraclePartition partition;
    private final OracleOffsetContext offsetContext;
    private final OracleStreamingChangeEventSourceMetrics metrics;

    /**
     * Cache of transactions, keyed based on the transaction's unique identifier
     */
    private final Map<String, MemoryTransaction> transactionCache = new HashMap<>();
    /**
     * Cache of processed transactions (committed or rolled back), keyed based on the transaction's unique identifier.
     */
    private final Map<String, Scn> recentlyProcessedTransactionsCache = new HashMap<>();
    private final Set<Scn> schemaChangesCache = new HashSet<>();
    private final Set<String> abandonedTransactionsCache = new HashSet<>();
    private final Map<String, MissingTransactionObservation> missingTransactionsCache = new HashMap<>();

    private static final int MISSING_TRANSACTION_CONFIRMATIONS_TO_ABANDON = 2;
    private static final Duration MISSING_TRANSACTION_CHECK_INTERVAL = Duration.ofSeconds(30);

    public MemoryLogMinerEventProcessor(ChangeEventSourceContext context,
                                        OracleConnectorConfig connectorConfig,
                                        OracleConnection jdbcConnection,
                                        EventDispatcher<OraclePartition, TableId> dispatcher,
                                        OraclePartition partition,
                                        OracleOffsetContext offsetContext,
                                        OracleDatabaseSchema schema,
                                        OracleStreamingChangeEventSourceMetrics metrics) {
        super(context, connectorConfig, schema, partition, offsetContext, dispatcher, metrics);
        this.jdbcConnection = jdbcConnection;
        this.dispatcher = dispatcher;
        this.partition = partition;
        this.offsetContext = offsetContext;
        this.metrics = metrics;
    }

    @Override
    protected Map<String, MemoryTransaction> getTransactionCache() {
        return transactionCache;
    }

    @Override
    protected OracleConnection getJdbcConnection() {
        return jdbcConnection;
    }

    @Override
    protected MemoryTransaction createTransaction(LogMinerEventRow row) {
        return new MemoryTransaction(row.getTransactionId(), row.getScn(), row.getChangeTime(), row.getUserName());
    }

    @Override
    protected void removeEventWithRowId(LogMinerEventRow row) {
        MemoryTransaction transaction = getTransactionCache().get(row.getTransactionId());
        if (transaction == null) {
            if (isTransactionIdWithNoSequence(row.getTransactionId())) {
                // This means that Oracle LogMiner found an event that should be undone but its corresponding
                // undo entry was read in a prior mining session and the transaction's sequence could not be
                // resolved. In this case, lets locate the transaction based solely on XIDUSN and XIDSLT.
                final String transactionPrefix = getTransactionIdPrefix(row.getTransactionId());
                LOGGER.debug("Undo change refers to a transaction that has no explicit sequence, '{}'", row.getTransactionId());
                LOGGER.debug("Checking all transactions with prefix '{}'", transactionPrefix);
                for (String transactionKey : getTransactionCache().keySet()) {
                    if (transactionKey.startsWith(transactionPrefix)) {
                        transaction = getTransactionCache().get(transactionKey);
                        if (transaction != null && transaction.removeEventWithRowId(row.getRowId())) {
                            // We successfully found a transaction with the same XISUSN and XIDSLT and that
                            // transaction included a change for the specified row id.
                            LOGGER.debug("Undo change '{}' applied to transaction '{}'", row, transactionKey);
                            return;
                        }
                    }
                }
                LOGGER.warn("Cannot undo change '{}' since event with row-id {} was not found.", row, row.getRowId());
            }
            else if (!getConfig().isLobEnabled()) {
                LOGGER.warn("Cannot undo change '{}' since transaction was not found.", row);
            }
        }
        else {
            if (!transaction.removeEventWithRowId(row.getRowId())) {
                LOGGER.warn("Cannot undo change '{}' since event with row-id {} was not found.", row, row.getRowId());
            }
        }
    }

    @Override
    public void close() throws Exception {
        // close any resources used here
    }

    @Override
    public void abandonTransactions(Duration retention) throws InterruptedException {
        if (!Duration.ZERO.equals(retention)) {
            final Scn offsetScn = offsetContext.getScn();
            Optional<Scn> lastScnToAbandonTransactions = getLastScnToAbandon(jdbcConnection, offsetScn, retention);
            if (lastScnToAbandonTransactions.isPresent()) {
                Scn thresholdScn = lastScnToAbandonTransactions.get();
                LOGGER.warn("All transactions with SCN <= {} will be abandoned.", thresholdScn);
                Scn smallestScn = getTransactionCacheMinimumScn();
                if (!smallestScn.isNull()) {
                    if (thresholdScn.compareTo(smallestScn) < 0) {
                        thresholdScn = smallestScn;
                    }

                    Iterator<Map.Entry<String, MemoryTransaction>> iterator = transactionCache.entrySet().iterator();
                    while (iterator.hasNext()) {
                        Map.Entry<String, MemoryTransaction> entry = iterator.next();
                        if (entry.getValue().getStartScn().compareTo(thresholdScn) <= 0) {
                            LOGGER.warn("Transaction {} is being abandoned.", entry.getKey());
                            abandonedTransactionsCache.add(entry.getKey());
                            iterator.remove();

                            metrics.addAbandonedTransactionId(entry.getKey());
                            metrics.setActiveTransactions(transactionCache.size());
                        }
                    }

                    // Update the oldest scn metric are transaction abandonment
                    smallestScn = getTransactionCacheMinimumScn();
                    metrics.setOldestScn(smallestScn.isNull() ? Scn.valueOf(-1) : smallestScn);
                }

                offsetContext.setScn(thresholdScn);
                dispatcher.dispatchHeartbeatEvent(partition, offsetContext);
            }
        }
    }

    @Override
    protected Scn abandonTransactionsAfterEmptyPlSqlOutputWindow(OraclePartition partition, Scn endScn) throws InterruptedException {
        abandonTransactions(getConfig().getLogMiningTransactionRetention());
        return abandonTransactionsMissingFromDatabase(partition, endScn);
    }

    private Scn abandonTransactionsMissingFromDatabase(OraclePartition partition, Scn endScn) throws InterruptedException {
        missingTransactionsCache.keySet().removeIf(transactionId -> !transactionCache.containsKey(transactionId));

        Scn terminalScn = Scn.NULL;
        for (String transactionId : new HashSet<>(transactionCache.keySet())) {
            final MemoryTransaction transaction = transactionCache.get(transactionId);
            if (transaction == null) {
                continue;
            }
            final MissingTransactionObservation observation = missingTransactionsCache.get(transactionId);
            if (observation != null && !shouldCheckMissingTransaction(endScn, observation)) {
                LOGGER.debug(
                        "PL/SQL output LogMiner keeping cached transaction {} while waiting to re-check V$TRANSACTION: startScn={}, endScn={}, missingObservedAtCurrentScn={}, confirmations={}",
                        transactionId, transaction.getStartScn(), endScn, observation.getObservedAtCurrentScn(), observation.getConfirmations());
                continue;
            }

            if (isTransactionActiveInDatabase(transactionId)) {
                missingTransactionsCache.remove(transactionId);
                continue;
            }

            final TerminalTransactionEventLookup terminalEventLookup = findTerminalTransactionEvent(transactionId, transaction.getStartScn(), endScn);
            if (terminalEventLookup.isFailed()) {
                continue;
            }
            if (terminalEventLookup.isPresent()) {
                final TerminalTransactionEvent event = terminalEventLookup.getEvent();
                if (event.getEventType() == EventType.ROLLBACK) {
                    LOGGER.info(
                            "PL/SQL output LogMiner found terminal ROLLBACK for cached transaction {} missing from V$TRANSACTION; removing rolled back transaction. startScn={}, events={}, rollbackScn={}, endScn={}",
                            transactionId, transaction.getStartScn(), transaction.getEvents().size(), event.getScn(), endScn);
                    transactionCache.remove(transactionId);
                    abandonedTransactionsCache.remove(transactionId);
                    missingTransactionsCache.remove(transactionId);
                    if (getConfig().isLobEnabled()) {
                        recentlyProcessedTransactionsCache.put(transactionId, event.getScn());
                    }
                    metrics.setActiveTransactions(transactionCache.size());
                    metrics.incrementRolledBackTransactions();
                    metrics.addRolledBackTransactionId(transactionId);
                    counters.rollbackCount++;
                    terminalScn = maxScn(terminalScn, event.getScn());
                    continue;
                }
                LOGGER.info(
                        "PL/SQL output LogMiner found terminal COMMIT for cached transaction {} missing from V$TRANSACTION; committing cached transaction. startScn={}, events={}, commitScn={}, endScn={}",
                        transactionId, transaction.getStartScn(), transaction.getEvents().size(), event.getScn(), endScn);
                handleCommit(partition, createTerminalEventRow(transactionId, event));
                terminalScn = maxScn(terminalScn, event.getScn());
                continue;
            }

            final MissingTransactionObservation updatedObservation = recordMissingTransactionObservation(transactionId, transaction, endScn, observation);
            if (updatedObservation.canAbandonAt(endScn)) {
                LOGGER.warn(
                        "PL/SQL output LogMiner abandoning stale cached transaction {} after repeated V$TRANSACTION misses and mined SCN watermark: startScn={}, events={}, endScn={}, missingObservedAtCurrentScn={}, confirmations={}",
                        transactionId, transaction.getStartScn(), transaction.getEvents().size(), endScn,
                        updatedObservation.getObservedAtCurrentScn(), updatedObservation.getConfirmations());
                abandonedTransactionsCache.add(transactionId);
                transactionCache.remove(transactionId);
                missingTransactionsCache.remove(transactionId);
                metrics.addAbandonedTransactionId(transactionId);
                metrics.setActiveTransactions(transactionCache.size());
            }
        }
        final Scn smallestScn = getTransactionCacheMinimumScn();
        metrics.setOldestScn(smallestScn.isNull() ? Scn.valueOf(-1) : smallestScn);
        return terminalScn;
    }

    private boolean shouldCheckMissingTransaction(Scn endScn, MissingTransactionObservation observation) {
        if (observation.hasBeenMinedThrough(endScn)) {
            return true;
        }
        return Duration.between(observation.getLastCheckedAt(), Instant.now()).compareTo(MISSING_TRANSACTION_CHECK_INTERVAL) >= 0;
    }

    private MissingTransactionObservation recordMissingTransactionObservation(String transactionId, MemoryTransaction transaction,
                                                                              Scn endScn, MissingTransactionObservation observation) {
        final Instant now = Instant.now();
        if (observation == null) {
            final Scn currentScn = getCurrentScnForMissingTransaction(transactionId);
            final MissingTransactionObservation newObservation = new MissingTransactionObservation(currentScn, now);
            missingTransactionsCache.put(transactionId, newObservation);
            LOGGER.info(
                    "PL/SQL output LogMiner observed cached transaction {} missing from V$TRANSACTION; keeping until mined through observed database SCN. startScn={}, events={}, endScn={}, missingObservedAtCurrentScn={}, confirmations={}",
                    transactionId, transaction.getStartScn(), transaction.getEvents().size(), endScn, currentScn, newObservation.getConfirmations());
            return newObservation;
        }
        if (observation.getObservedAtCurrentScn().isNull()) {
            final Scn currentScn = getCurrentScnForMissingTransaction(transactionId);
            final MissingTransactionObservation newObservation = new MissingTransactionObservation(currentScn, now);
            missingTransactionsCache.put(transactionId, newObservation);
            LOGGER.info(
                    "PL/SQL output LogMiner refreshed missing transaction {} database SCN watermark after an earlier current SCN lookup failure. startScn={}, events={}, endScn={}, missingObservedAtCurrentScn={}, confirmations={}",
                    transactionId, transaction.getStartScn(), transaction.getEvents().size(), endScn, currentScn, newObservation.getConfirmations());
            return newObservation;
        }

        observation.confirm(now);
        LOGGER.info(
                "PL/SQL output LogMiner confirmed cached transaction {} still missing from V$TRANSACTION. startScn={}, events={}, endScn={}, missingObservedAtCurrentScn={}, confirmations={}, minedThroughObservedScn={}",
                transactionId, transaction.getStartScn(), transaction.getEvents().size(), endScn,
                observation.getObservedAtCurrentScn(), observation.getConfirmations(), observation.hasBeenMinedThrough(endScn));
        return observation;
    }

    private Scn getCurrentScnForMissingTransaction(String transactionId) {
        try {
            return jdbcConnection.getCurrentScn();
        }
        catch (SQLException e) {
            LOGGER.warn("Unable to read current SCN while checking missing transaction {}; using NULL watermark and keeping it in cache.", transactionId, e);
            metrics.incrementWarningCount();
            return Scn.NULL;
        }
    }

    private boolean isTransactionActiveInDatabase(String transactionId) {
        try (PreparedStatement statement = jdbcConnection.connection().prepareStatement(
                "SELECT COUNT(1) FROM V$TRANSACTION WHERE RAWTOHEX(XID) = UPPER(?)")) {
            statement.setString(1, transactionId);
            final Integer count;
            try (ResultSet rs = statement.executeQuery()) {
                count = rs.next() ? rs.getInt(1) : 0;
            }
            return count != null && count > 0;
        }
        catch (SQLException e) {
            LOGGER.warn("Unable to check whether transaction {} is active in V$TRANSACTION; keeping it in cache.", transactionId, e);
            metrics.incrementWarningCount();
            return true;
        }
    }

    @Override
    protected boolean isRecentlyProcessed(String transactionId) {
        return recentlyProcessedTransactionsCache.containsKey(transactionId);
    }

    @Override
    protected boolean hasSchemaChangeBeenSeen(LogMinerEventRow row) {
        return schemaChangesCache.contains(row.getScn());
    }

    @Override
    protected MemoryTransaction getAndRemoveTransactionFromCache(String transactionId) {
        missingTransactionsCache.remove(transactionId);
        return getTransactionCache().remove(transactionId);
    }

    @Override
    protected void removeTransactionAndEventsFromCache(MemoryTransaction transaction) {
        abandonedTransactionsCache.remove(transaction.getTransactionId());
        missingTransactionsCache.remove(transaction.getTransactionId());
    }

    @Override
    protected Iterator<LogMinerEvent> getTransactionEventIterator(MemoryTransaction transaction) {
        return transaction.getEvents().iterator();
    }

    @Override
    protected void finalizeTransactionCommit(String transactionId, Scn commitScn) {
        if (getConfig().isLobEnabled()) {
            // cache recently committed transactions by transaction id
            recentlyProcessedTransactionsCache.put(transactionId, commitScn);
        }
    }

    @Override
    protected void finalizeTransactionRollback(String transactionId, Scn rollbackScn) {
        transactionCache.remove(transactionId);
        abandonedTransactionsCache.remove(transactionId);
        missingTransactionsCache.remove(transactionId);
        if (getConfig().isLobEnabled()) {
            recentlyProcessedTransactionsCache.put(transactionId, rollbackScn);
        }
    }

    @Override
    protected void handleSchemaChange(LogMinerEventRow row) throws InterruptedException {
        super.handleSchemaChange(row);
        if (row.getTableName() != null && getConfig().isLobEnabled()) {
            schemaChangesCache.add(row.getScn());
        }
    }

    @Override
    protected void addToTransaction(String transactionId, LogMinerEventRow row, Supplier<LogMinerEvent> eventSupplier) {
        if (abandonedTransactionsCache.contains(transactionId)) {
            LOGGER.warn("Event for abandoned transaction {}, skipped.", transactionId);
            return;
        }
        if (!isRecentlyProcessed(transactionId)) {
            MemoryTransaction transaction = getTransactionCache().get(transactionId);
            if (transaction == null) {
                LOGGER.trace("Transaction {} not in cache for DML, creating.", transactionId);
                transaction = createTransaction(row);
                getTransactionCache().put(transactionId, transaction);
            }
            missingTransactionsCache.remove(transactionId);

            int eventId = transaction.getNextEventId();
            if (transaction.getEvents().size() <= eventId) {
                // Add new event at eventId offset
                LOGGER.trace("Transaction {}, adding event reference at index {}", transactionId, eventId);
                transaction.getEvents().add(eventSupplier.get());
                metrics.calculateLagMetrics(row.getChangeTime());
            }

            metrics.setActiveTransactions(getTransactionCache().size());
        }
        else if (!getConfig().isLobEnabled()) {
            // Explicitly only log this warning when LobEnabled is false because its commonplace for a
            // transaction to be re-mined and therefore seen as already processed until the SCN low
            // watermark is advanced after a long transaction is committed.
            LOGGER.warn("Event for transaction {} has already been processed, skipped.", transactionId);
        }
    }

    @Override
    protected int getTransactionEventCount(MemoryTransaction transaction) {
        return transaction.getEvents().size();
    }

    @Override
    protected PreparedStatement createQueryStatement() throws SQLException {
        final boolean plSqlOutput = OracleConnectorConfig.LogMiningStrategy.PLSQL_OUTPUT.equals(getConfig().getLogMiningStrategy());
        final String query = plSqlOutput
                ? LogMinerQueryBuilder.buildPlSqlOutputBlock(getConfig(), getSchema())
                : LogMinerQueryBuilder.build(getConfig(), getSchema());
        if (plSqlOutput) {
            LOGGER.info("Creating PL/SQL output LogMiner statement using memory buffer.");
        }
        return jdbcConnection.connection().prepareStatement(query,
                ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY,
                ResultSet.HOLD_CURSORS_OVER_COMMIT);
    }

    @Override
    protected Scn calculateNewStartScn(Scn endScn, Scn maxCommittedScn) throws InterruptedException {
        if (getConfig().isLobEnabled()) {
            if (transactionCache.isEmpty() && !maxCommittedScn.isNull()) {
                offsetContext.setScn(maxCommittedScn);
                dispatcher.dispatchHeartbeatEvent(partition, offsetContext);
            }
            else {
                abandonTransactions(getConfig().getLogMiningTransactionRetention());
                final Scn minStartScn = getTransactionCacheMinimumScn();
                if (!minStartScn.isNull()) {
                    recentlyProcessedTransactionsCache.entrySet().removeIf(entry -> entry.getValue().compareTo(minStartScn) < 0);
                    schemaChangesCache.removeIf(scn -> scn.compareTo(minStartScn) < 0);
                    offsetContext.setScn(minStartScn.subtract(Scn.valueOf(1)));
                    dispatcher.dispatchHeartbeatEvent(partition, offsetContext);
                }
            }
            return offsetContext.getScn();
        }
        else {
            if (!getLastProcessedScn().isNull() && getLastProcessedScn().compareTo(endScn) < 0) {
                // If the last processed SCN is before the endScn we need to use the last processed SCN as the
                // next starting point as the LGWR buffer didn't flush all entries from memory to disk yet.
                endScn = getLastProcessedScn();
            }

            if (transactionCache.isEmpty()) {
                offsetContext.setScn(endScn);
                dispatcher.dispatchHeartbeatEvent(partition, offsetContext);
            }
            else {
                abandonTransactions(getConfig().getLogMiningTransactionRetention());
                final Scn minStartScn = getTransactionCacheMinimumScn();
                if (!minStartScn.isNull()) {
                    offsetContext.setScn(minStartScn.subtract(Scn.valueOf(1)));
                    dispatcher.dispatchHeartbeatEvent(partition, offsetContext);
                }
            }
            return endScn;
        }
    }

    /**
     * Calculates the SCN as a watermark to abandon for long running transactions.
     * The criteria is do not let the offset SCN expire from archives older the specified retention hours.
     *
     * @param connection database connection, should not be {@code null}
     * @param offsetScn offset system change number, should not be {@code null}
     * @param retention duration to tolerate long running transactions before being abandoned, must not be {@code null}
     * @return an optional system change number as the watermark for transaction buffer abandonment
     */
    protected Optional<Scn> getLastScnToAbandon(OracleConnection connection, Scn offsetScn, Duration retention) {
        try {
            Float diffInDays = connection.singleOptionalValue(SqlUtils.diffInDaysQuery(offsetScn), rs -> rs.getFloat(1));
            if (diffInDays != null && (diffInDays * 24) > retention.toHours()) {
                return Optional.of(offsetScn);
            }
            return Optional.empty();
        }
        catch (SQLException e) {
            LOGGER.error("Cannot calculate days difference for transaction abandonment", e);
            metrics.incrementErrorCount();
            return Optional.of(offsetScn);
        }
    }

    @Override
    protected Scn getTransactionCacheMinimumScn() {
        return transactionCache.values().stream()
                .map(MemoryTransaction::getStartScn)
                .min(Scn::compareTo)
                .orElse(Scn.NULL);
    }

    private static class MissingTransactionObservation {
        private final Scn observedAtCurrentScn;
        private int confirmations;
        private Instant lastCheckedAt;

        private MissingTransactionObservation(Scn observedAtCurrentScn, Instant lastCheckedAt) {
            this.observedAtCurrentScn = observedAtCurrentScn;
            this.lastCheckedAt = lastCheckedAt;
            this.confirmations = 1;
        }

        private Scn getObservedAtCurrentScn() {
            return observedAtCurrentScn;
        }

        private int getConfirmations() {
            return confirmations;
        }

        private Instant getLastCheckedAt() {
            return lastCheckedAt;
        }

        private void confirm(Instant checkedAt) {
            this.confirmations++;
            this.lastCheckedAt = checkedAt;
        }

        private boolean hasBeenMinedThrough(Scn endScn) {
            return !observedAtCurrentScn.isNull() && endScn.compareTo(observedAtCurrentScn) >= 0;
        }

        private boolean canAbandonAt(Scn endScn) {
            return confirmations >= MISSING_TRANSACTION_CONFIRMATIONS_TO_ABANDON && hasBeenMinedThrough(endScn);
        }
    }
}
