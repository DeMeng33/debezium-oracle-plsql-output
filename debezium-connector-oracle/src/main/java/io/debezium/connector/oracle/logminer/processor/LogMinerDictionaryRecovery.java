/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.processor;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.BooleanSupplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.logminer.events.LogMinerEventRow;

/**
 * Verifies empty PL/SQL windows independently of the online dictionary. Only a complete,
 * unambiguously mapped SMON_SCN_TIME SCN may be consumed as transaction metadata.
 * No transaction or offset is changed here; all validation and session changes precede replay.
 */
class LogMinerDictionaryRecovery implements AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(LogMinerDictionaryRecovery.class);
    private static final int MAX_BOUNDARY_ROWS = 100_000;
    private static final String RAW_QUERY = "SELECT q.* FROM (SELECT ROWNUM AS ROW_SEQUENCE, SCN, OPERATION_CODE, DATA_OBJ#, "
            + "RAWTOHEX(XID) AS XID_HEX, RS_ID, SSN, CSF, STATUS, INFO, TIMESTAMP AS CHANGE_TIME, THREAD# AS REDO_THREAD "
            + "FROM V$LOGMNR_CONTENTS WHERE SCN > ? AND SCN <= ?) q ORDER BY q.SCN, q.ROW_SEQUENCE";

    private final Connection onlineConnection;
    private final Connection connection;
    private final OracleConnectorConfig config;
    private final BooleanSupplier running;
    private boolean diagnosticSessionAllocated;
    private final String recoveryId;
    private String stage = "NOT_STARTED";
    private List<String> files = Collections.emptyList();
    private final List<MiningLogFile> onlineLogFiles = new ArrayList<>();
    private final List<MiningLogFile> diagnosticLogFiles = new ArrayList<>();
    private boolean onlineLogFileListChanged;
    private long expectedRows = -1;
    private long actualRows;
    private String lastInspectedRow;
    private final Map<String, String> diagnosticMappings = new HashMap<>();
    private final Map<String, Set<String>> objectCandidates = new LinkedHashMap<>();
    private final Map<String, String> resolvedObjects = new HashMap<>();
    private final List<String> dictionaryChecks = new ArrayList<>();
    private Map<String, String> dictionaryContainers = Collections.emptyMap();
    private String dictionarySession;
    private String dictionaryContext;
    private Scn anomalyScn;
    private String sessionIdentity;

    LogMinerDictionaryRecovery(Connection onlineConnection, Connection connection, OracleConnectorConfig config, BooleanSupplier running) {
        this(onlineConnection, connection, config, running, UUID.randomUUID().toString());
    }

    LogMinerDictionaryRecovery(Connection onlineConnection, Connection connection, OracleConnectorConfig config, BooleanSupplier running, String recoveryId) {
        if (onlineConnection == connection) {
            throw new IllegalArgumentException("LogMiner diagnostics require a separate JDBC connection");
        }
        this.onlineConnection = onlineConnection;
        this.connection = connection;
        this.config = config;
        this.running = running;
        this.recoveryId = recoveryId;
    }

    Plan verify(Scn start, Scn end) throws SQLException, InterruptedException {
        try {
            return verifyWindow(start, end);
        }
        catch (SQLException | RuntimeException | InterruptedException e) {
            LOGGER.error("LOGMINER_DIAGNOSIS_FAILED recoveryId={}, stage={}, range=({}, {}], logFiles={}, expectedRawRows={}, actualRawRows={}, "
                    + "lastInspectedRow={}, objectMappings={}, sessionIdentity={}, onlineLogFiles={}, diagnosticLogFiles={}, anomalyScn={}, "
                    + "dictionarySession={}, dictionaryContext={}, dictionaryContainers={}, dictionaryChecks={}, noDiscardDecisionApplied=true",
                    recoveryId, stage, start, end, files, expectedRows, actualRows, lastInspectedRow, diagnosticMappings, sessionIdentity,
                    onlineLogFiles, diagnosticLogFiles, anomalyScn, dictionarySession, dictionaryContext, dictionaryContainers, dictionaryChecks, e);
            throw e;
        }
    }

    private Plan verifyWindow(Scn start, Scn end) throws SQLException, InterruptedException {
        checkRunning();
        if (config.isContinuousMining()) {
            throw new DebeziumException("Raw LogMiner verification requires an explicit, fixed log file set; CONTINUOUS_MINE cannot guarantee it");
        }
        stage = "SESSION_IDENTITY";
        verifyConnectionIdentity();
        stage = "LOG_FILE_SNAPSHOT";
        snapshotLogFiles(onlineConnection, onlineLogFiles);
        final List<MiningLogFile> selected = selectLogFiles(onlineLogFiles, start, end, "A");
        files = fileNames(selected);
        LOGGER.warn("Verifying online zero-row/ORA-01403 window with OPTIONS=0: scnRange=({}, {}], logFiles={}", start, end, files);
        restart(files, start, end, false);
        stage = "LOG_FILE_VERIFY_B";
        snapshotLogFiles(connection, diagnosticLogFiles);
        selectLogFiles(diagnosticLogFiles, start, end, "B");
        verifyLogFileIdentity(selected);
        stage = "RAW_COUNT";
        final long expected = count(connection, start, end);
        expectedRows = expected;
        if (expected == 0) {
            // A has not been changed by B's diagnosis. Leave its online session intact;
            // the normal streaming loop will start the next window on A.
            LOGGER.info("Verified physical empty LogMiner window: scnRange=({}, {}], rawRows=0", start, end);
            return new Plan(end, false, Collections.emptyList());
        }

        final Set<String> knownObjects = knownObjects();
        stage = "RAW_SCAN";
        Scn firstBadScn = null;
        long rows = 0;
        long boundaryRows = 0;
        String requiredOnlineRow = null;
        final Map<String, RawRow> filteredObjects = new HashMap<>();
        try (PreparedStatement statement = connection.prepareStatement(RAW_QUERY)) {
            statement.setString(1, start.toString());
            statement.setString(2, end.toString());
            statement.setFetchSize(config.getLogMiningViewFetchSize());
            try (ResultSet result = statement.executeQuery()) {
                while (result.next()) {
                    checkRunning();
                    RawRow row = new RawRow(result, config.getCatalogName());
                    rows++;
                    actualRows = rows;
                    lastInspectedRow = row.toString();
                    if (row.code != 6) {
                        if (row.isDml() || row.code == 0) {
                            filteredObjects.putIfAbsent(row.mappingKey(), row);
                        }
                        else {
                            requiredOnlineRow = row.toString();
                        }
                    }
                    if (row.event.getStatus() != 0 && row.event.getStatus() != 2) {
                        requiredOnlineRow = "unexpected raw status: " + row;
                    }
                    if (row.code == 34) {
                        throw unsafe("missing redo", row);
                    }
                    if (firstBadScn == null && row.isDml() && knownObjects.contains(row.objectId)) {
                        firstBadScn = row.scn;
                        anomalyScn = firstBadScn;
                    }
                    if (row.scn.equals(start.add(Scn.ONE))) {
                        boundaryRows++;
                    }
                }
            }
        }
        if (rows != expected) {
            throw new DebeziumException("Incomplete raw LogMiner cursor: range=(" + start + ", " + end
                    + "], expectedRows=" + expected + ", actualRows=" + rows + "; refusing offset advancement");
        }
        LOGGER.info("Raw LogMiner verification: scnRange=({}, {}], rawRows={}, firstBadScn={}, knownDataObjects={}",
                start, end, rows, firstBadScn, knownObjects);
        if (firstBadScn == null) {
            restart(files, start, end, true);
            stage = "ONLINE_COUNT";
            // A filtered online query can legitimately return no captured rows in a physically
            // nonempty window. Raw evidence alone cannot distinguish that from decoder failure.
            // This aggregate is diagnostic only, and must agree with the independent raw scan.
            if (count(onlineConnection, start, end) != expected) {
                throw new DebeziumException("Online/raw LogMiner row count mismatch; refusing offset advancement for (" + start + ", " + end + "]");
            }
            // The independent raw scan must also show that zero captured output is possible.
            // COMMIT/ROLLBACK are unconditionally selected online; their absence cannot be
            // explained by a table filter. Only unambiguously mapped SYS DML/INTERNAL and
            // START rows may be absent from the filtered online output.
            String emptyFailure = requiredOnlineRow == null ? null : "raw redo requires an online row: " + requiredOnlineRow;
            if (emptyFailure == null && !filteredObjects.isEmpty()) {
                prepareObjectMappings(filteredObjects.values());
                for (RawRow row : filteredObjects.values()) {
                    try {
                        String object = resolveObject(row);
                        if (!object.startsWith("SYS.") || (row.event.getStatus() != 0 && row.event.getStatus() != 2)) {
                            emptyFailure = "raw redo contains a potentially captured or invalid operation: " + row + ", object=" + object;
                            break;
                        }
                    }
                    catch (DebeziumException e) {
                        emptyFailure = e.getMessage();
                        break;
                    }
                }
            }
            LOGGER.info("Online/raw counts agree; requiring filtered online replay: scnRange=({}, {}], rawRows={}, emptyReplayRejection={}",
                    start, end, rows, emptyFailure);
            return new Plan(end, true, Collections.emptyList(), emptyFailure);
        }
        if (config.isLobEnabled()) {
            throw new DebeziumException("SMON_SCN_TIME metadata recovery does not support LOB re-mining; refusing offset advancement at SCN " + firstBadScn);
        }
        if (firstBadScn.compareTo(start.add(Scn.ONE)) > 0) {
            Scn safeEnd = firstBadScn.subtract(Scn.ONE);
            restart(files, start, safeEnd, true);
            return new Plan(safeEnd, true, Collections.emptyList());
        }

        if (boundaryRows > MAX_BOUNDARY_ROWS) {
            throw new DebeziumException("Metadata SCN exceeds validation row limit " + MAX_BOUNDARY_ROWS + ": SCN=" + firstBadScn);
        }
        final List<RawRow> boundary = new ArrayList<>();
        stage = "BOUNDARY_SCAN";
        try (PreparedStatement statement = connection.prepareStatement(RAW_QUERY)) {
            statement.setString(1, start.toString());
            statement.setString(2, firstBadScn.toString());
            statement.setFetchSize(config.getLogMiningViewFetchSize());
            try (ResultSet result = statement.executeQuery()) {
                while (result.next()) {
                    checkRunning();
                    if (boundary.size() == MAX_BOUNDARY_ROWS) {
                        throw new DebeziumException("Metadata SCN grew beyond validation row limit: SCN=" + firstBadScn);
                    }
                    boundary.add(new RawRow(result, config.getCatalogName()));
                }
            }
        }
        if (boundary.size() != boundaryRows) {
            throw new DebeziumException("Incomplete metadata SCN: SCN=" + firstBadScn + ", expectedRows=" + boundaryRows + ", actualRows=" + boundary.size());
        }
        lastInspectedRow = boundary.isEmpty() ? null : boundary.get(0).toString();
        prepareObjectMappings(boundary);
        stage = "BOUNDARY_VALIDATION";
        List<LogMinerEventRow> boundaries = validateBoundary(boundary);
        final List<String> records = new ArrayList<>();
        final Map<Integer, Long> operations = new HashMap<>();
        int discardedDml = 0;
        int discardedNonDml = 0;
        for (RawRow row : boundary) {
            operations.merge(row.code, 1L, Long::sum);
            final String action;
            if (row.code == 7 || row.code == 36) {
                action = "PRESERVE_TRANSACTION_BOUNDARY";
            }
            else {
                action = row.isDml() ? "DISCARD_CONFIRMED_SYS_DML" : "IGNORE_NON_CAPTURED_OPERATION";
                if (row.isDml()) {
                    discardedDml++;
                }
                else {
                    discardedNonDml++;
                }
            }
            records.add("action=" + action + ", " + row + ", objectMapping="
                    + (row.objectId == null ? "NOT_APPLICABLE" : diagnosticMappings.getOrDefault(row.mappingKey(), "NOT_REQUIRED_FOR_BOUNDARY")));
        }
        final Audit audit = new Audit(recoveryId, records, "requestedRange=(" + start + ", " + end + "], anomalyScn=" + firstBadScn
                + ", rawWindowRows=" + expected + ", anomalyRows=" + boundary.size() + ", discardedSysDml=" + discardedDml
                + ", ignoredNonDml=" + discardedNonDml + ", preservedBoundaries=" + boundaries.size() + ", operations=" + operations
                + ", logFiles=" + files + ", objectMappings=" + diagnosticMappings + ", sessionIdentity=" + sessionIdentity
                + ", dictionarySession=" + dictionarySession + ", dictionaryContext=" + dictionaryContext
                + ", dictionaryContainers=" + dictionaryContainers + ", dictionaryChecks=" + dictionaryChecks);
        // Restore before emitting any commit. A failed END/ADD/START must not advance offsets.
        restart(files, start, firstBadScn, true);
        return new Plan(firstBadScn, false, boundaries, null, audit);
    }

    private List<LogMinerEventRow> validateBoundary(List<RawRow> rows) {
        final Map<String, String> mappings = new HashMap<>();
        final List<LogMinerEventRow> boundaries = new ArrayList<>();
        int ignoredDml = 0;
        for (RawRow row : rows) {
            lastInspectedRow = row.toString();
            if (row.code == 7 || row.code == 36) {
                if (row.event.getTransactionId() == null || row.event.getThread() <= 0 || row.event.getChangeTime() == null || row.event.getStatus() != 0
                        || row.csf != 0) {
                    throw unsafe("incomplete transaction boundary", row);
                }
                boundaries.add(row.event);
                LOGGER.info("Validated raw transaction boundary: {}", row);
            }
            else if (row.code == 6) {
                // START has no captured DML. Existing transactions remain in the processor cache.
                continue;
            }
            else if (row.isDml() || row.code == 0) {
                String object = mappings.get(row.mappingKey());
                if (object == null) {
                    object = resolveObject(row);
                    mappings.put(row.mappingKey(), object);
                }
                LOGGER.info("Raw metadata object mapping: {}, object={}", row, object);
                if (row.event.getStatus() != 0 && row.event.getStatus() != 2) {
                    throw unsafe("unexpected raw redo status", row);
                }
                if (row.isDml() && "SYS.SMON_SCN_TIME".equals(object)) {
                    ignoredDml++;
                }
                else if (row.code != 0 || !object.startsWith("SYS.")) {
                    throw unsafe("captured or unsupported object operation on " + object, row);
                }
            }
            else {
                throw unsafe("unsupported operation (including DDL/LOB)", row);
            }
        }
        if (ignoredDml == 0) {
            throw new DebeziumException("No verified SMON_SCN_TIME DML at metadata boundary; refusing offset advancement");
        }
        LOGGER.info("Validated complete metadata SCN: scn={}, rawRows={}, ignoredSysDml={}, commitRollbackRows={}",
                rows.get(0).scn, rows.size(), ignoredDml, boundaries.size());
        return boundaries;
    }

    private String resolveObject(RawRow row) {
        lastInspectedRow = row.toString();
        if (row.objectId == null) {
            throw unsafe("missing DATA_OBJ#", row);
        }
        // This deployment captures C##XHKJ objects only from CDB$ROOT. The DBA confirmed
        // that C##XHKJ owns no tables in PDB$SEED or PDBORCL and that this will remain true.
        // Re-enable per-container mapping before using this recovery in a deployment that
        // captures PDB objects.
        Set<String> identities = objectCandidates.getOrDefault(row.mappingKey(), Collections.emptySet());
        if (identities.size() != 1 || !resolvedObjects.containsKey(row.mappingKey())) {
            diagnosticMappings.put(row.mappingKey(), "UNRESOLVED:" + identities);
            throw unsafe("unmapped or ambiguous " + row.referenceType() + "; candidates=" + identities, row);
        }
        diagnosticMappings.put(row.mappingKey(), identities.toString());
        return resolvedObjects.get(row.mappingKey());
    }

    private void prepareObjectMappings(Collection<RawRow> rows) throws SQLException, InterruptedException {
        final Map<String, RawRow> objects = new LinkedHashMap<>();
        for (RawRow row : rows) {
            if (row.isDml() || row.code == 0) {
                objects.putIfAbsent(row.mappingKey(), row);
            }
        }
        if (objects.isEmpty()) {
            return;
        }
        checkRunning();
        lastInspectedRow = objects.values().iterator().next().toString();
        stage = "ROOT_DICTIONARY_SCOPE";
        final boolean cdb = isCdb();
        try {
            final List<String> context = dictionaryContext(connection);
            final String containerId = context.get(0);
            final String containerName = context.get(1);
            dictionarySession = "source=B, identity=" + connectionIdentity(connection)
                    + ", fields=[CON_ID,CON_NAME,SESSION_USER,INSTANCE_NAME,SID], context=" + context;
            dictionaryContext = "scope=ROOT_ONLY, capturedOwner=C##XHKJ, pdbCapturedTables=NONE_CONFIRMED, "
                    + "crossContainerLookupSkipped=true, actualContext=" + context;
            if (cdb && (!"1".equals(containerId) || !"CDB$ROOT".equals(containerName))) {
                throw new DebeziumException("Root-only dictionary verification requires B in CDB$ROOT: " + context);
            }
            dictionaryContainers = Collections.singletonMap(containerId, containerName);
            LOGGER.warn("LOGMINER_ROOT_DICTIONARY_SCOPE recoveryId={}, deploymentInvariant=capturedOwner C##XHKJ has no tables in any PDB, "
                    + "checkedContainer={}, crossContainerLookupSkipped=true, setContainerRequired=false", recoveryId, dictionaryContainers);
            stage = "ROOT_DICTIONARY_OBJECT_QUERY";
            try (PreparedStatement statement = connection.prepareStatement("SELECT DISTINCT SYS_CONTEXT('USERENV','CON_ID'), o.OWNER, "
                    + "CASE WHEN o.OBJECT_TYPE = 'CLUSTER' THEN t.TABLE_NAME ELSE o.OBJECT_NAME END, "
                    + "o.OBJECT_ID, o.DATA_OBJECT_ID, o.OBJECT_TYPE "
                    + "FROM DBA_OBJECTS o LEFT JOIN DBA_TABLES t ON o.OBJECT_TYPE = 'CLUSTER' "
                    + "AND t.OWNER = o.OWNER AND t.CLUSTER_NAME = o.OBJECT_NAME "
                    + "WHERE (o.DATA_OBJECT_ID = ? OR (? = 1 AND o.OBJECT_ID = ?)) AND o.DATA_OBJECT_ID IS NOT NULL")) {
                for (RawRow row : objects.values()) {
                    checkRunning();
                    lastInspectedRow = row.toString();
                    if (row.objectId == null) {
                        continue;
                    }
                    Set<String> local = new LinkedHashSet<>();
                    List<String> matches = new ArrayList<>();
                    Set<String> candidates = objectCandidates.computeIfAbsent(row.mappingKey(), key -> new LinkedHashSet<>());
                    statement.setString(1, row.objectId);
                    statement.setInt(2, row.code == 0 ? 1 : 0);
                    statement.setString(3, row.objectId);
                    try (ResultSet result = statement.executeQuery()) {
                        while (result.next()) {
                            checkRunning();
                            if (!containerId.equals(result.getString(1)) || result.getString(2) == null || result.getString(3) == null) {
                                throw unsafe("unresolved cluster member or invalid root object identity", row);
                            }
                            String resultObjectId = result.getString(4);
                            String resultDataObjectId = result.getString(5);
                            boolean matchedDataObject = row.objectId.equals(resultDataObjectId);
                            boolean matchedObject = row.code == 0 && row.objectId.equals(resultObjectId);
                            if (!matchedDataObject && !matchedObject) {
                                throw unsafe("dictionary returned an object that did not match the raw reference", row);
                            }
                            String mapped = result.getString(2) + "." + result.getString(3);
                            String identity = containerId + ":" + mapped;
                            local.add(identity);
                            candidates.add(identity);
                            resolvedObjects.put(row.mappingKey(), mapped);
                            diagnosticMappings.put(row.mappingKey(), "PARTIAL:" + candidates);
                            String matchedBy = matchedDataObject && matchedObject ? "DATA_OBJECT_ID_AND_OBJECT_ID"
                                    : matchedDataObject ? "DATA_OBJECT_ID" : "OBJECT_ID_FALLBACK";
                            matches.add("matchedBy=" + matchedBy + ", OBJECT_ID=" + resultObjectId + ", DATA_OBJECT_ID=" + resultDataObjectId
                                    + ", OBJECT_TYPE=" + result.getString(6) + ", identity=" + identity);
                        }
                    }
                    String evidence = "container=" + containerId + "=" + containerName + ", referenceType=" + row.referenceType()
                            + ", DATA_OBJ#=" + row.objectId + ", candidates=" + local + ", matches=" + matches;
                    dictionaryChecks.add(evidence);
                    LOGGER.info("LOGMINER_ROOT_DICTIONARY_OBJECT_CHECK recoveryId={}, {}, dictionarySession={}, rawRow={}",
                            recoveryId, evidence, dictionarySession, row);
                }
            }
        }
        catch (SQLException | RuntimeException | InterruptedException e) {
            LOGGER.error("LOGMINER_ROOT_DICTIONARY_VALIDATION_FAILED recoveryId={}, stage={}, anomalyScn={}, lastInspectedRow={}, dictionarySession={}, "
                    + "dictionaryContext={}, containers={}, completedChecks={}, partialMappings={}, sqlState={}, oracleErrorCode={}, noDiscardDecisionApplied=true",
                    recoveryId, stage, anomalyScn, lastInspectedRow, dictionarySession, dictionaryContext, dictionaryContainers, dictionaryChecks, diagnosticMappings,
                    e instanceof SQLException ? ((SQLException) e).getSQLState() : null, e instanceof SQLException ? ((SQLException) e).getErrorCode() : null, e);
            throw e;
        }
        LOGGER.info("LOGMINER_ROOT_DICTIONARY_SCAN_COMPLETED recoveryId={}, containers={}, checks={}, crossContainerLookupSkipped=true",
                recoveryId, dictionaryContainers, dictionaryChecks);
    }

    private List<String> dictionaryContext(Connection target) throws SQLException {
        try (PreparedStatement statement = target.prepareStatement("SELECT SYS_CONTEXT('USERENV','CON_ID'), SYS_CONTEXT('USERENV','CON_NAME'), "
                + "SYS_CONTEXT('USERENV','SESSION_USER'), SYS_CONTEXT('USERENV','INSTANCE_NAME'), SYS_CONTEXT('USERENV','SID') FROM DUAL");
                ResultSet result = statement.executeQuery()) {
            if (!result.next()) {
                throw new DebeziumException("Cannot identify dictionary container/session");
            }
            List<String> values = new ArrayList<>();
            for (int i = 1; i <= 5; i++) {
                String value = result.getString(i);
                if (value == null || value.isEmpty()) {
                    throw new DebeziumException("Incomplete dictionary container/session at column " + i);
                }
                values.add(value);
            }
            return values;
        }
    }

    private boolean isCdb() throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("SELECT CDB FROM V$DATABASE");
                ResultSet result = statement.executeQuery()) {
            if (!result.next()) {
                throw new DebeziumException("Cannot determine LogMiner database container scope");
            }
            String cdb = result.getString(1);
            if (!"YES".equals(cdb) && !"NO".equals(cdb)) {
                throw new DebeziumException("Unknown LogMiner database container scope: " + cdb);
            }
            return "YES".equals(cdb);
        }
    }

    private Set<String> knownObjects() throws SQLException {
        Set<String> ids = new HashSet<>();
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT DATA_OBJECT_ID FROM DBA_OBJECTS WHERE OWNER = 'SYS' AND OBJECT_NAME = 'SMON_SCN_TIME' AND OBJECT_TYPE = 'TABLE'");
                ResultSet result = statement.executeQuery()) {
            while (result.next()) {
                if (result.getString(1) != null) {
                    ids.add(result.getString(1));
                }
            }
        }
        return ids;
    }

    private void snapshotLogFiles(Connection source, List<MiningLogFile> snapshot) throws SQLException {
        try (PreparedStatement statement = source.prepareStatement("SELECT FILENAME, STATUS, INFO, LOW_SCN, NEXT_SCN, THREAD_ID, THREAD_SQN, TYPE "
                + "FROM V$LOGMNR_LOGS ORDER BY THREAD_ID, LOW_SCN, FILENAME");
                ResultSet result = statement.executeQuery()) {
            while (result.next()) {
                snapshot.add(new MiningLogFile(result));
            }
        }
    }

    private List<MiningLogFile> selectLogFiles(List<MiningLogFile> snapshot, Scn start, Scn end, String session) {
        if (snapshot.isEmpty()) {
            throw new DebeziumException("Cannot recover without the LogMiner file set: session=" + session);
        }
        final Scn first = start.add(Scn.ONE);
        final List<MiningLogFile> selected = new ArrayList<>();
        final Map<Integer, List<MiningLogFile>> byThread = new LinkedHashMap<>();
        String failure = null;
        for (MiningLogFile file : snapshot) {
            final String action;
            final String reason;
            if (file.low.isNull() || file.next.isNull() || file.low.compareTo(Scn.valueOf(0)) < 0 || file.next.compareTo(file.low) <= 0
                    || file.thread <= 0 || file.sequence.isNull() || file.sequence.compareTo(Scn.valueOf(0)) <= 0) {
                action = "REJECT";
                reason = "INCOMPLETE_OR_INVALID_FILE_METADATA";
            }
            else {
                byThread.computeIfAbsent(file.thread, key -> new ArrayList<>());
                // Logs use [LOW_SCN, NEXT_SCN); mining uses (start, end].
                if (file.next.compareTo(first) <= 0 || file.low.compareTo(end) > 0) {
                    action = "EXCLUDE";
                    reason = "OUTSIDE_REQUESTED_WINDOW";
                }
                else if (file.name == null || file.name.isEmpty() || !file.isReadable() || "MISSING_LOGFILE".equals(file.info)) {
                    action = "REJECT";
                    reason = "REQUIRED_FILE_UNAVAILABLE";
                }
                else {
                    action = "SELECT";
                    reason = "OVERLAPS_REQUESTED_WINDOW";
                    selected.add(file);
                    byThread.get(file.thread).add(file);
                }
            }
            final String details = "recoveryId=" + recoveryId + ", session=" + session + ", range=(" + start + ", " + end
                    + "], action=" + action + ", reason=" + reason + ", " + file + ", sessionIdentity=" + sessionIdentity;
            if ("REJECT".equals(action)) {
                LOGGER.error("LOGMINER_LOG_FILE_DECISION {}", details);
                failure = details;
            }
            else if ("EXCLUDE".equals(action)) {
                if (!file.isReadable() && !Integer.valueOf(2).equals(file.status)) {
                    LOGGER.error("LOGMINER_LOG_FILE_DECISION {}, windowDiscarded=false", details);
                }
                else {
                    LOGGER.warn("LOGMINER_LOG_FILE_DECISION {}, windowDiscarded=false", details);
                }
            }
            else {
                LOGGER.info("LOGMINER_LOG_FILE_DECISION {}", details);
            }
        }
        if (failure != null) {
            throw new DebeziumException("Cannot recover with missing or invalid LogMiner log file: " + failure);
        }
        // Never combine one thread's coverage with another's to hide missing redo.
        for (Map.Entry<Integer, List<MiningLogFile>> entry : byThread.entrySet()) {
            Scn coveredUntil = first;
            entry.getValue().sort(Comparator.comparing(file -> file.low));
            for (MiningLogFile file : entry.getValue()) {
                if (file.low.compareTo(coveredUntil) > 0) {
                    break;
                }
                if (file.next.compareTo(coveredUntil) > 0) {
                    coveredUntil = file.next;
                }
            }
            if (coveredUntil.compareTo(end) <= 0) {
                throw new DebeziumException("Incomplete LogMiner log coverage: recoveryId=" + recoveryId + ", session=" + session
                        + ", range=(" + start + ", " + end + "], thread=" + entry.getKey() + ", firstUncoveredScn=" + coveredUntil
                        + ", threadFiles=" + entry.getValue() + "; refusing offset advancement");
            }
        }
        return selected;
    }

    private void verifyLogFileIdentity(List<MiningLogFile> selected) {
        // STATUS may change from 0 to 1 after START; compare file identity, not its processing role.
        Set<List<Object>> expected = new HashSet<>();
        Set<List<Object>> actual = new HashSet<>();
        for (MiningLogFile file : selected) {
            expected.add(file.identity());
        }
        for (MiningLogFile file : diagnosticLogFiles) {
            actual.add(file.identity());
        }
        if (!expected.equals(actual)) {
            throw new DebeziumException("Diagnostic LogMiner file set differs from the selected online window file set: expected=" + selected
                    + ", actual=" + diagnosticLogFiles);
        }
    }

    private List<String> fileNames(List<MiningLogFile> logs) {
        List<String> names = new ArrayList<>();
        for (MiningLogFile file : logs) {
            if (!names.contains(file.name)) {
                names.add(file.name);
            }
        }
        return names;
    }

    boolean isOnlineLogFileListChanged() {
        return onlineLogFileListChanged;
    }

    private void restart(List<String> files, Scn start, Scn end, boolean online) throws SQLException, InterruptedException {
        checkRunning();
        stage = online ? "RESTART_ONLINE_A" : "START_RAW_B";
        final Connection target = online ? onlineConnection : connection;
        if (online) {
            try (CallableStatement statement = target.prepareCall("BEGIN DBMS_LOGMNR.END_LOGMNR; END;")) {
                statement.execute();
            }
        }
        else {
            // B is newly opened for this verification. ADD_LOGFILE(NEW) establishes its
            // session; END_LOGMNR before the first ADD would raise ORA-01307.
            diagnosticSessionAllocated = true;
        }
        for (int i = 0; i < files.size(); i++) {
            try (CallableStatement statement = target.prepareCall("BEGIN DBMS_LOGMNR.ADD_LOGFILE(LOGFILENAME => ?, OPTIONS => DBMS_LOGMNR."
                    + (i == 0 ? "NEW" : "ADDFILE") + "); END;")) {
                statement.setString(1, files.get(i));
                statement.execute();
            }
        }
        if (online && !new HashSet<>(files).equals(new HashSet<>(fileNames(onlineLogFiles)))) {
            onlineLogFileListChanged = true;
            LOGGER.info("LOGMINER_ONLINE_FILE_SET_CHANGED recoveryId={}, range=({}, {}], originalFiles={}, selectedFiles={}, reloadBeforeNextWindow=true",
                    recoveryId, start, end, onlineLogFiles, files);
        }
        try (CallableStatement statement = target.prepareCall("BEGIN DBMS_LOGMNR.START_LOGMNR(STARTSCN => ?, ENDSCN => ?, OPTIONS => "
                + (online ? "DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG + DBMS_LOGMNR.NO_ROWID_IN_STMT" : "0") + "); END;")) {
            statement.setString(1, start.add(Scn.ONE).toString());
            statement.setString(2, end.toString());
            statement.execute();
        }
    }

    private long count(Connection source, Scn start, Scn end) throws SQLException {
        // Translate NO_DATA_FOUND here so JDBC cannot misinterpret SQLCODE=100 as EOF.
        try (CallableStatement statement = source.prepareCall("BEGIN SELECT COUNT(*) INTO ? FROM V$LOGMNR_CONTENTS "
                + "WHERE SCN > ? AND SCN <= ?; EXCEPTION WHEN NO_DATA_FOUND THEN "
                + "RAISE_APPLICATION_ERROR(-20004, 'LogMiner cursor raised NO_DATA_FOUND during completeness check'); END;")) {
            statement.registerOutParameter(1, Types.NUMERIC);
            statement.setString(2, start.toString());
            statement.setString(3, end.toString());
            statement.execute();
            if (statement.getBigDecimal(1) == null) {
                throw new DebeziumException("Missing LogMiner completeness count");
            }
            return statement.getBigDecimal(1).longValueExact();
        }
    }

    private void verifyConnectionIdentity() throws SQLException {
        List<String> online = connectionIdentity(onlineConnection);
        List<String> diagnostic = connectionIdentity(connection);
        sessionIdentity = "fields=[DBID,RESETLOGS_CHANGE#,CON_ID,INSTANCE_NAME,SID], A=" + online + ", B=" + diagnostic;
        if (!online.subList(0, 4).equals(diagnostic.subList(0, 4)) || online.get(4).equals(diagnostic.get(4))) {
            throw new DebeziumException("LogMiner connections must be distinct sessions on the same database incarnation, container and instance: "
                    + "online=" + online + ", diagnostic=" + diagnostic);
        }
    }

    private List<String> connectionIdentity(Connection source) throws SQLException {
        try (PreparedStatement statement = source.prepareStatement("SELECT DBID, RESETLOGS_CHANGE#, SYS_CONTEXT('USERENV','CON_ID'), "
                + "SYS_CONTEXT('USERENV','INSTANCE_NAME'), SYS_CONTEXT('USERENV','SID') FROM V$DATABASE");
                ResultSet result = statement.executeQuery()) {
            if (!result.next()) {
                throw new DebeziumException("Cannot identify LogMiner database session");
            }
            List<String> identity = new ArrayList<>();
            for (int i = 1; i <= 5; i++) {
                String value = result.getString(i);
                if (value == null) {
                    throw new DebeziumException("Incomplete LogMiner database session identity at column " + i);
                }
                identity.add(value);
            }
            return identity;
        }
    }

    @Override
    public void close() throws SQLException {
        if (diagnosticSessionAllocated) {
            try (CallableStatement statement = connection.prepareCall("BEGIN DBMS_LOGMNR.END_LOGMNR; END;")) {
                statement.execute();
            }
            catch (SQLException e) {
                if (e.getErrorCode() != 1307) {
                    LOGGER.error("LOGMINER_DIAGNOSTIC_CLEANUP_FAILED recoveryId={}, logFiles={}, sessionIdentity={}", recoveryId, files, sessionIdentity, e);
                    throw e;
                }
            }
            finally {
                diagnosticSessionAllocated = false;
            }
        }
        // The caller owns the JDBC connection and closes it after this LogMiner cleanup.
        // Never close or end A here, including when verification fails.
    }

    private void checkRunning() throws InterruptedException {
        if (!running.getAsBoolean() || Thread.currentThread().isInterrupted()) {
            throw new InterruptedException("LogMiner metadata verification cancelled; refusing offset advancement");
        }
    }

    private static DebeziumException unsafe(String reason, RawRow row) {
        return new DebeziumException("Unsafe LogMiner metadata fallback: " + reason + "; " + row + "; refusing offset advancement");
    }

    private static class MiningLogFile {
        final String name;
        final Integer status;
        final String info;
        final Scn low;
        final Scn next;
        final int thread;
        final Scn sequence;
        final String type;

        MiningLogFile(ResultSet result) throws SQLException {
            name = result.getString("FILENAME");
            int value = result.getInt("STATUS");
            status = result.wasNull() ? null : value;
            info = result.getString("INFO");
            low = scn(result.getString("LOW_SCN"));
            next = scn(result.getString("NEXT_SCN"));
            thread = result.getInt("THREAD_ID");
            sequence = scn(result.getString("THREAD_SQN"));
            type = result.getString("TYPE");
        }

        private static Scn scn(String value) {
            return value == null ? Scn.NULL : Scn.valueOf(value);
        }

        boolean isReadable() {
            // Oracle 12.1 V$LOGMNR_LOGS: 0=Will be read, 1=First to be read, 2=Not needed, 4=Missing log file.
            return Integer.valueOf(0).equals(status) || Integer.valueOf(1).equals(status);
        }

        List<Object> identity() {
            return java.util.Arrays.asList(name, low, next, thread, sequence, type);
        }

        @Override
        public String toString() {
            return "FILENAME=" + name + ", STATUS=" + status + ", INFO=" + info + ", LOW_SCN=" + low + ", NEXT_SCN=" + next
                    + ", THREAD_ID=" + thread + ", THREAD_SQN=" + sequence + ", TYPE=" + type;
        }
    }

    static class Plan {
        final Scn end;
        final boolean replayOnline;
        final List<LogMinerEventRow> boundaries;
        final String emptyReplayRejection;
        final Audit audit;

        Plan(Scn end, boolean replayOnline, List<LogMinerEventRow> boundaries) {
            this(end, replayOnline, boundaries, null);
        }

        Plan(Scn end, boolean replayOnline, List<LogMinerEventRow> boundaries, String emptyReplayRejection) {
            this(end, replayOnline, boundaries, emptyReplayRejection, null);
        }

        Plan(Scn end, boolean replayOnline, List<LogMinerEventRow> boundaries, String emptyReplayRejection, Audit audit) {
            this.end = end;
            this.replayOnline = replayOnline;
            this.boundaries = boundaries;
            this.emptyReplayRejection = emptyReplayRejection;
            this.audit = audit;
        }
    }

    static class Audit {
        final String recoveryId;
        final List<String> records;
        final String summary;

        Audit(String recoveryId, List<String> records, String summary) {
            this.recoveryId = recoveryId;
            this.records = Collections.unmodifiableList(new ArrayList<>(records));
            this.summary = summary;
        }
    }

    private static class RawRow {
        final Scn scn;
        final int code;
        final String objectId;
        final int csf;
        final LogMinerEventRow event;

        RawRow(ResultSet result, String catalog) throws SQLException {
            scn = Scn.valueOf(result.getString("SCN"));
            code = result.getInt("OPERATION_CODE");
            objectId = result.getString("DATA_OBJ#");
            csf = result.getInt("CSF");
            String xid = result.getString("XID_HEX");
            Timestamp timestamp = result.getTimestamp("CHANGE_TIME");
            event = LogMinerEventRow.fromValues(catalog, scn, null, code,
                    timestamp == null ? null : timestamp.toLocalDateTime().toInstant(ZoneOffset.UTC),
                    xid == null || xid.isEmpty() ? null : xid.toLowerCase(Locale.ROOT),
                    null, null, null, null, null, false, result.getString("RS_ID"), result.getInt("STATUS"),
                    result.getString("INFO"), result.getInt("SSN"), result.getInt("REDO_THREAD"));
        }

        boolean isDml() {
            return code == 1 || code == 2 || code == 3;
        }

        String mappingKey() {
            return referenceType() + objectId;
        }

        String referenceType() {
            return code == 0 ? "INTERNAL_OBJECT_OR_DATA#=" : "DATA_OBJECT#=";
        }

        @Override
        public String toString() {
            return "scn=" + scn + ", operationCode=" + code + ", DATA_OBJ#=" + objectId + ", xid=" + event.getTransactionId()
                    + ", rsId=" + event.getRsId() + ", ssn=" + event.getSsn() + ", csf=" + csf + ", redoThread=" + event.getThread()
                    + ", changeTime=" + event.getChangeTime() + ", status=" + event.getStatus() + ", info=" + event.getInfo();
        }
    }
}
