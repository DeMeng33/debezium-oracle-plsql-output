/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.processor;

import static org.fest.assertions.Assertions.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.junit.Test;
import org.mockito.stubbing.Answer;

import io.debezium.DebeziumException;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.logminer.events.EventType;
import io.debezium.junit.logging.LogInterceptor;

public class LogMinerDictionaryRecoveryTest {
    @Test
    public void shouldVerifyPhysicalEmptyWindowUsingExactFilesAndBoundsWithoutChangingA() throws Exception {
        Database db = new Database();
        LogMinerDictionaryRecovery.Plan plan = db.verify(100, 120);
        assertThat(plan.end).isEqualTo(Scn.valueOf(120));
        assertThat(plan.replayOnline).isFalse();
        assertThat(db.addedFiles).containsExactly("archive-a", "archive-b");
        assertThat(db.starts).containsExactly("raw:101:120");
        assertThat(db.sessionCalls.stream().anyMatch(call -> call.startsWith("A:"))).isFalse();
    }

    @Test
    public void shouldDiagnoseDeployedArchiveWindowWithoutUnneededOnlineLog() throws Exception {
        Database db = new Database();
        db.logFiles = Arrays.asList(logFile("archive-36828", 1, 234243386, 234357425, 1, 36828),
                logFile("online-36829", 2, 234357425, 281474976710655L, 1, 36829));
        LogInterceptor logs = new LogInterceptor(LogMinerDictionaryRecovery.class);
        try {
            assertThat(db.verify(234309575, 234329575).end).isEqualTo(Scn.valueOf(234329575));
            assertThat(db.addedFiles).containsExactly("archive-36828");
            assertThat(db.starts).containsExactly("raw:234309576:234329575");
            assertThat(db.sessionCalls.stream().anyMatch(call -> call.startsWith("A:"))).isFalse();
            assertThat(logs.containsWarnMessage("action=EXCLUDE, reason=OUTSIDE_REQUESTED_WINDOW, FILENAME=online-36829, STATUS=2")).isTrue();
            assertThat(logs.containsErrorMessage("LOGMINER_DIAGNOSIS_FAILED")).isFalse();
        }
        finally {
            logs.stop();
        }
    }

    @Test
    public void shouldExcludeUnavailableLogsOnlyWhenTheirRangesAreOutsideWindow() throws Exception {
        for (Integer status : Arrays.asList(0, 1, 2, 4, 99, null)) {
            Database db = new Database();
            db.logFiles = Arrays.asList(logFile("old", status, 80, 101, 1, 1), logFile("required", 0, 101, 121, 1, 2),
                    logFile("future", status, 121, 150, 1, 3));
            assertThat(db.verify(100, 120).end).isEqualTo(Scn.valueOf(120));
            assertThat(db.addedFiles).containsExactly("required");
        }
    }

    @Test
    public void shouldRejectUnavailableRequiredFileAndAuditAllFileMetadata() throws Exception {
        for (Integer status : Arrays.asList(2, 4, 99, null)) {
            Database db = new Database();
            db.logFiles = Arrays.asList(logFile("required", status, 90, 130, 1, 36828), logFile("future", 2, 130, 150, 1, 36829));
            db.logFiles.get(0).put("INFO", "file-state-detail");
            LogInterceptor logs = new LogInterceptor(LogMinerDictionaryRecovery.class);
            try {
                expectUnsafe(db, "REQUIRED_FILE_UNAVAILABLE");
                assertThat(db.starts).isEmpty();
                assertThat(db.countModes).isEmpty();
                assertThat(logs.containsErrorMessage("FILENAME=required, STATUS=" + status + ", INFO=file-state-detail, LOW_SCN=90, NEXT_SCN=130, "
                        + "THREAD_ID=1, THREAD_SQN=36828, TYPE=ARCHIVED")).isTrue();
                assertThat(logs.containsErrorMessage("onlineLogFiles=[")).isTrue();
                assertThat(logs.containsErrorMessage("FILENAME=future")).isTrue();
                assertThat(logs.containsErrorMessage("noDiscardDecisionApplied=true")).isTrue();
            }
            finally {
                logs.stop();
            }
        }
    }

    @Test
    public void shouldRejectMissingRequiredFileNameAndMissingLogInfo() throws Exception {
        Database db = new Database();
        db.logFiles = Collections.singletonList(logFile(null, 0, 90, 130, 1, 1));
        expectUnsafe(db, "REQUIRED_FILE_UNAVAILABLE");
        db.logFiles = Collections.singletonList(logFile("required", 0, 90, 130, 1, 1));
        db.logFiles.get(0).put("INFO", "MISSING_LOGFILE");
        expectUnsafe(db, "REQUIRED_FILE_UNAVAILABLE");
        assertThat(db.starts).isEmpty();
    }

    @Test
    public void shouldRejectUnknownOrInvalidLogRangesBeforeMining() throws Exception {
        for (String column : Arrays.asList("LOW_SCN", "NEXT_SCN", "THREAD_ID", "THREAD_SQN")) {
            Database db = new Database();
            db.logFiles.get(1).put(column, null);
            expectUnsafe(db, "INCOMPLETE_OR_INVALID_FILE_METADATA");
            assertThat(db.starts).isEmpty();
        }
        Database db = new Database();
        db.logFiles = Collections.singletonList(logFile("invalid", 0, 150, 100, 1, 1));
        expectUnsafe(db, "INCOMPLETE_OR_INVALID_FILE_METADATA");
    }

    @Test
    public void shouldRejectEmptyFileSnapshot() throws Exception {
        Database db = new Database();
        db.logFiles = Collections.emptyList();
        expectUnsafe(db, "without the LogMiner file set");
        assertThat(db.starts).isEmpty();
    }

    @Test
    public void shouldAcceptContiguousCoverageAndIncludeFileStartingAtWindowEnd() throws Exception {
        Database db = new Database();
        db.logFiles = Arrays.asList(logFile("first", 1, 90, 120, 1, 1), logFile("last", 0, 120, 130, 1, 2));
        assertThat(db.verify(100, 120).end).isEqualTo(Scn.valueOf(120));
        assertThat(db.addedFiles).containsExactly("first", "last");
    }

    @Test
    public void shouldRejectStartMiddleAndEndCoverageGaps() throws Exception {
        Database db = new Database();
        db.logFiles = Collections.singletonList(logFile("late-start", 0, 102, 130, 1, 1));
        expectUnsafe(db, "firstUncoveredScn=101");
        db.logFiles = Arrays.asList(logFile("first", 1, 90, 110, 1, 1), logFile("last", 0, 111, 130, 1, 2));
        expectUnsafe(db, "firstUncoveredScn=110");
        db.logFiles = Collections.singletonList(logFile("early-end", 0, 90, 120, 1, 1));
        expectUnsafe(db, "firstUncoveredScn=120");
        assertThat(db.starts).isEmpty();
    }

    @Test
    public void shouldNotUseAnotherRedoThreadToFillCoverageGaps() throws Exception {
        Database db = new Database();
        db.logFiles = Arrays.asList(logFile("thread-1", 0, 90, 130, 1, 1), logFile("thread-2", 0, 110, 130, 2, 1));
        expectUnsafe(db, "thread=2, firstUncoveredScn=101");
        assertThat(db.starts).isEmpty();
        db.logFiles = Arrays.asList(logFile("thread-1", 0, 90, 130, 1, 1), logFile("thread-2", 0, 90, 130, 2, 1));
        assertThat(db.verify(100, 120).end).isEqualTo(Scn.valueOf(120));
        assertThat(db.addedFiles).containsExactly("thread-1", "thread-2");
    }

    @Test
    public void shouldRejectThreadWithOnlyOutOfWindowLogs() throws Exception {
        Database db = new Database();
        db.logFiles = Arrays.asList(logFile("thread-1", 0, 90, 130, 1, 1), logFile("thread-2-future", 2, 130, 150, 2, 2));
        expectUnsafe(db, "thread=2, firstUncoveredScn=101");
        assertThat(db.starts).isEmpty();
    }

    @Test
    public void shouldCompareFileIdentityIndependentlyOfOrderAndReadableStatus() throws Exception {
        Database db = new Database();
        db.diagnosticFiles = Arrays.asList(logFile("archive-b", 0, 110, 130, 1, 2), logFile("archive-a", 1, 90, 110, 1, 1));
        assertThat(db.verify(100, 120).end).isEqualTo(Scn.valueOf(120));
    }

    @Test
    public void shouldRejectReusedFileNameWithChangedSequenceOrRangeOnB() throws Exception {
        for (String column : Arrays.asList("THREAD_SQN", "LOW_SCN", "NEXT_SCN")) {
            Database db = new Database();
            db.logFiles = Collections.singletonList(logFile("online", 0, 90, 130, 1, 1));
            Map<Object, Object> changed = logFile("online", 0, 90, 130, 1, 1);
            changed.put(column, "LOW_SCN".equals(column) ? "91" : "131");
            db.diagnosticFiles = Collections.singletonList(changed);
            expectUnsafe(db, "file set differs");
            assertThat(db.countModes).isEmpty();
            assertThat(db.diagnosticSessionAllocated).isFalse();
        }
    }

    @Test
    public void shouldRejectRequiredFileThatBecomesUnavailableOnB() throws Exception {
        Database db = new Database();
        db.diagnosticFiles = Arrays.asList(logFile("archive-a", 1, 90, 110, 1, 1), logFile("archive-b", 4, 110, 130, 1, 2));
        expectUnsafe(db, "session=B");
        assertThat(db.countModes).isEmpty();
        assertThat(db.diagnosticSessionAllocated).isFalse();
    }

    @Test
    public void shouldRejectUnexpectedFileOnBEvenOutsideWindow() throws Exception {
        Database db = new Database();
        db.diagnosticFiles = new ArrayList<>(db.logFiles);
        db.diagnosticFiles.add(logFile("unexpected", 2, 130, 150, 1, 3));
        expectUnsafe(db, "file set differs");
        assertThat(db.countModes).isEmpty();
    }

    @Test
    public void shouldRequireOnlineReplayForFilteredNonemptyWindow() throws Exception {
        Database db = new Database(row(110, 6, null));
        assertThat(db.verify(100, 120).replayOnline).isTrue();
        assertThat(db.countModes).containsExactly("raw", "online");
    }

    @Test
    public void shouldNotAdvanceUnknownOnlineDictionaryFailure() throws Exception {
        Database db = new Database(row(110, 1, "999"));
        db.failOnlineCount = true;
        try {
            db.verify(100, 120);
            fail("Expected online completeness failure");
        }
        catch (SQLException e) {
            assertThat(e.getErrorCode()).isEqualTo(20004);
        }
    }

    @Test
    public void shouldReplayBusinessAndCommitBeforeFirstBadScnOnline() throws Exception {
        Database db = new Database(row(101, 3, "999"), row(102, 7, null), row(110, 1, "9876"));
        LogMinerDictionaryRecovery.Plan plan = db.verify(100, 120);
        assertThat(plan.replayOnline).isTrue();
        assertThat(plan.end).isEqualTo(Scn.valueOf(109));
        assertThat(plan.boundaries).isEmpty();
        assertThat(db.starts).containsExactly("raw:101:120", "online:101:109");
    }

    @Test
    public void shouldPreserveCommitAndRollbackAtSysScnInRedoOrder() throws Exception {
        Map<Object, Object> commit = row(101, 7, null);
        commit.put("XID_HEX", "ABCDEF");
        Database db = new Database(row(101, 6, null), row(101, 1, "9876"), commit, row(101, 36, null));
        LogMinerDictionaryRecovery.Plan plan = db.verify(100, 120);
        assertThat(plan.end).isEqualTo(Scn.valueOf(101));
        assertThat(plan.boundaries).hasSize(2);
        assertThat(plan.boundaries.get(0).getEventType()).isEqualTo(EventType.COMMIT);
        assertThat(plan.boundaries.get(0).getTransactionId()).isEqualTo("abcdef");
        assertThat(plan.boundaries.get(0).getThread()).isEqualTo(1);
        assertThat(plan.boundaries.get(1).getEventType()).isEqualTo(EventType.ROLLBACK);
    }

    @Test
    public void shouldRecoverConsecutiveAndLaterIndependentSysScns() throws Exception {
        Database db = new Database(row(101, 1, "9876"), row(102, 1, "9876"), row(110, 1, "9876"));
        assertThat(db.verify(100, 120).end).isEqualTo(Scn.valueOf(101));
        assertThat(db.verify(101, 120).end).isEqualTo(Scn.valueOf(102));
        LogMinerDictionaryRecovery.Plan prefix = db.verify(102, 120);
        assertThat(prefix.replayOnline).isTrue();
        assertThat(prefix.end).isEqualTo(Scn.valueOf(109));
        assertThat(db.verify(109, 120).end).isEqualTo(Scn.valueOf(110));
    }

    @Test
    public void shouldRejectBusinessDmlEvenAfterCommitWasReadAtSameScn() throws Exception {
        Database db = new Database(row(101, 1, "9876"), row(101, 7, null), row(101, 3, "999"));
        db.objects.put("999", Collections.singletonList(object(1, "APP", "CAPTURED_TABLE")));
        expectUnsafe(db, "captured or unsupported object");
    }

    @Test
    public void shouldRejectUnmappedObject() throws Exception {
        expectUnsafe(new Database(row(101, 1, "9876"), row(101, 3, "999")), "unmapped or ambiguous");
    }

    @Test
    public void shouldRejectMultiTableClusterWithinRoot() throws Exception {
        Database db = new Database(row(101, 1, "9876"));
        db.objects.put("9876", Arrays.asList(object(1, "SYS", "SMON_SCN_TIME"), object(1, "SYS", "ANOTHER_MEMBER")));
        expectUnsafe(db, "ambiguous");
    }

    @Test
    public void shouldAcceptClusterAliasForSameSingleMember() throws Exception {
        Database db = new Database(row(101, 1, "9876"));
        db.objects.put("9876", Arrays.asList(object(1, "SYS", "SMON_SCN_TIME"), object(1, "SYS", "SMON_SCN_TIME")));
        assertThat(db.verify(100, 120).end).isEqualTo(Scn.valueOf(101));
    }

    @Test
    public void shouldValidateInternalSysObjectAndRejectUnknownOperations() throws Exception {
        Database db = new Database(row(101, 1, "9876"), row(101, 0, "9875"));
        db.objects.put("9875", Collections.singletonList(object(1, "SYS", "PENDING_SUB_SESSIONS$")));
        assertThat(db.verify(100, 120).end).isEqualTo(Scn.valueOf(101));
        expectUnsafe(new Database(row(101, 1, "9876"), row(101, 5, "9876")), "unsupported operation");
        expectUnsafe(new Database(row(101, 1, "9876"), row(101, 34, null)), "missing redo");
    }

    @Test
    public void shouldRejectIncompleteRawCursor() throws Exception {
        Database db = new Database(row(101, 1, "9876"));
        db.countAdjustment = 1;
        expectUnsafe(db, "Incomplete raw LogMiner cursor");
    }

    @Test
    public void shouldNotReturnValidatedBoundariesIfOnlineRestoreFails() throws Exception {
        Database db = new Database(row(101, 1, "9876"), row(101, 7, null));
        db.failRestore = true;
        try {
            db.verify(100, 120);
            fail("Expected restore failure");
        }
        catch (SQLException e) {
            assertThat(e.getMessage()).contains("restore failed");
        }
    }

    @Test
    public void shouldRejectUnsupportedRecoveryModesAndCancelledVerification() throws Exception {
        Database db = new Database(row(101, 1, "9876"));
        when(db.config.isLobEnabled()).thenReturn(true);
        expectUnsafe(db, "does not support LOB");
        when(db.config.isContinuousMining()).thenReturn(true);
        expectUnsafe(db, "CONTINUOUS_MINE");
        try {
            new LogMinerDictionaryRecovery(db.connection, db.diagnosticConnection, db.config, () -> false)
                    .verify(Scn.valueOf(100), Scn.valueOf(120));
            fail("Expected cancellation");
        }
        catch (InterruptedException expected) {
            assertThat(expected.getMessage()).contains("cancelled");
        }
    }

    @Test
    public void shouldResolveOnlyRootDictionaryForConfirmedRootOnlyDeployment() throws Exception {
        Database db = new Database(row(101, 1, "9876"));
        LogMinerDictionaryRecovery.Plan plan = db.verify(100, 120);
        assertThat(plan.end).isEqualTo(Scn.valueOf(101));
        assertThat(db.dictionaryQueries).containsExactly("1:DATA_OBJECT#=9876");
        assertThat(plan.audit.summary).contains("scope=ROOT_ONLY").contains("pdbCapturedTables=NONE_CONFIRMED")
                .contains("container=1=CDB$ROOT");
        assertThat(db.sql.stream().noneMatch(query -> query.startsWith("ALTER SESSION SET CONTAINER"))).isTrue();
    }

    @Test
    public void shouldResolveDeployed345OnlyInRootAndKeepCommit() throws Exception {
        Database db = new Database(row(234316224, 1, "345"), row(234316224, 7, null));
        db.knownObjectId = "345";
        db.objects.put("345", Arrays.asList(object(1, "SYS", "SMON_SCN_TIME"), object(1, "SYS", "SMON_SCN_TIME")));
        db.logFiles = Collections.singletonList(logFile("archive-36828", 0, 234243386, 234357425, 1, 36828));
        LogMinerDictionaryRecovery.Plan plan = db.verify(234316223, 234338223);
        assertThat(plan.end).isEqualTo(Scn.valueOf(234316224));
        assertThat(plan.boundaries).hasSize(1);
        assertThat(db.dictionaryQueries).containsExactly("1:DATA_OBJECT#=345");
        assertThat(plan.audit.summary).contains("DATA_OBJECT#=345=[1:SYS.SMON_SCN_TIME]").contains("crossContainerLookupSkipped");
    }

    @Test
    public void shouldResolveDeployedInternal347ByObjectIdFallback() throws Exception {
        Database db = new Database(row(101, 2, "345"), row(101, 0, "346"), row(101, 0, "347"));
        db.knownObjectId = "345";
        db.objects.put("345", Collections.singletonList(object(1, "SYS", "SMON_SCN_TIME", "347", "345", "TABLE")));
        db.objects.put("346", Collections.singletonList(object(1, "SYS", "SMON_SCN_TO_TIME_AUX_IDX", "346", "346", "INDEX")));
        db.objects.put("347", Collections.singletonList(object(1, "SYS", "SMON_SCN_TIME", "347", "345", "TABLE")));
        LogMinerDictionaryRecovery.Plan plan = db.verify(100, 120);
        assertThat(plan.end).isEqualTo(Scn.valueOf(101));
        assertThat(db.dictionaryQueries).containsExactly("1:DATA_OBJECT#=345", "1:INTERNAL_OBJECT_OR_DATA#=346",
                "1:INTERNAL_OBJECT_OR_DATA#=347");
        assertThat(plan.audit.summary).contains("INTERNAL_OBJECT_OR_DATA#=347=[1:SYS.SMON_SCN_TIME]")
                .contains("matchedBy=OBJECT_ID_FALLBACK, OBJECT_ID=347, DATA_OBJECT_ID=345, OBJECT_TYPE=TABLE");
        assertThat(plan.audit.records.get(2)).contains("IGNORE_NON_CAPTURED_OPERATION").contains("SYS.SMON_SCN_TIME");
    }

    @Test
    public void shouldNotUseObjectIdFallbackForDml() throws Exception {
        Database db = new Database(row(101, 2, "347"));
        db.knownObjectId = "347";
        db.objects.put("347", Collections.singletonList(object(1, "SYS", "SMON_SCN_TIME", "347", "345", "TABLE")));
        expectUnsafe(db, "unmapped or ambiguous DATA_OBJECT#=");
        assertThat(db.dictionaryQueries).containsExactly("1:DATA_OBJECT#=347");
    }

    @Test
    public void shouldRejectInternalBusinessObjectResolvedByObjectIdFallback() throws Exception {
        Database db = new Database(row(101, 2, "345"), row(101, 0, "347"));
        db.knownObjectId = "345";
        db.objects.put("345", Collections.singletonList(object(1, "SYS", "SMON_SCN_TIME", "347", "345", "TABLE")));
        db.objects.put("347", Collections.singletonList(object(1, "APP", "BUSINESS_TABLE", "347", "999", "TABLE")));
        expectUnsafe(db, "captured or unsupported object operation on APP.BUSINESS_TABLE");
    }

    @Test
    public void shouldRejectAmbiguousInternalObjectAndDataObjectMatches() throws Exception {
        Database db = new Database(row(101, 2, "345"), row(101, 0, "347"));
        db.knownObjectId = "345";
        db.objects.put("345", Collections.singletonList(object(1, "SYS", "SMON_SCN_TIME", "347", "345", "TABLE")));
        db.objects.put("347", Arrays.asList(object(1, "SYS", "SMON_SCN_TIME", "347", "345", "TABLE"),
                object(1, "APP", "COLLIDING_SEGMENT", "999", "347", "TABLE")));
        expectUnsafe(db, "unmapped or ambiguous INTERNAL_OBJECT_OR_DATA#=");
    }

    @Test
    public void shouldAuditUnreadableRootDictionaryWithoutRestartingA() throws Exception {
        Database db = new Database(row(101, 1, "9876"));
        db.failRootDictionary = true;
        LogInterceptor logs = new LogInterceptor(LogMinerDictionaryRecovery.class);
        try {
            db.verify(100, 120);
            fail("Expected root dictionary failure");
        }
        catch (SQLException expected) {
            assertThat(expected.getErrorCode()).isEqualTo(942);
            assertThat(logs.containsErrorMessage("stage=ROOT_DICTIONARY_OBJECT_QUERY")).isTrue();
            assertThat(logs.containsErrorMessage("scope=ROOT_ONLY")).isTrue();
            assertThat(logs.containsErrorMessage("C##XHKJ")).isTrue();
            assertThat(logs.containsErrorMessage("sqlState=42000, oracleErrorCode=942")).isTrue();
            assertThat(logs.containsErrorMessage("anomalyScn=101")).isTrue();
            assertThat(logs.containsErrorMessage("noDiscardDecisionApplied=true")).isTrue();
        }
        finally {
            logs.stop();
        }
        assertThat(db.diagnosticSessionAllocated).isFalse();
        assertThat(db.starts).containsExactly("raw:101:120");
    }

    @Test
    public void shouldRequireDiagnosticConnectionToRemainInRootForCdb() throws Exception {
        Database db = new Database(row(101, 1, "9876"));
        db.diagnosticContainerId = "3";
        db.diagnosticContainerName = "PDBORCL";
        expectUnsafe(db, "requires B in CDB$ROOT");
        assertThat(db.dictionaryQueries).isEmpty();
    }

    @Test
    public void shouldRejectMissingClusterMember() throws Exception {
        Database db = new Database(row(101, 1, "9876"));
        db.objects.put("9876", Collections.singletonList(object(1, "SYS", null)));
        expectUnsafe(db, "unresolved cluster member");
    }

    @Test
    public void shouldReadLocalNonCdbDictionaryWithoutSwitchingContainers() throws Exception {
        Database db = new Database(row(101, 1, "9876"));
        db.cdb = false;
        db.objects.put("9876", Collections.singletonList(object(0, "SYS", "SMON_SCN_TIME")));
        assertThat(db.verify(100, 120).end).isEqualTo(Scn.valueOf(101));
        assertThat(db.dictionaryQueries).containsExactly("0:DATA_OBJECT#=9876");
    }

    @Test
    public void shouldCloseRawMiningOnCancellationDuringRootMapping() throws Exception {
        Database db = new Database(row(101, 1, "9876"));
        try (LogMinerDictionaryRecovery recovery = new LogMinerDictionaryRecovery(db.connection, db.diagnosticConnection,
                db.config, () -> db.dictionaryQueries.isEmpty())) {
            recovery.verify(Scn.valueOf(100), Scn.valueOf(120));
            fail("Expected cancellation during local object query");
        }
        catch (InterruptedException expected) {
            assertThat(expected.getMessage()).contains("cancelled");
        }
        assertThat(db.diagnosticSessionAllocated).isFalse();
        assertThat(db.starts).containsExactly("raw:101:120");
    }

    @Test
    public void shouldVerifyFilteredSysWindowWithLocalDictionaries() throws Exception {
        Database db = new Database(row(110, 1, "999"));
        db.objects.put("999", Collections.singletonList(object(1, "SYS", "OTHER_SYSTEM_TABLE")));
        LogMinerDictionaryRecovery.Plan plan = db.verify(100, 120);
        assertThat(plan.replayOnline).isTrue();
        assertThat(plan.emptyReplayRejection).isNull();
        assertThat(db.dictionaryQueries).containsExactly("1:DATA_OBJECT#=999");
    }

    @Test
    public void shouldKeepDictionaryModesIsolatedAndOnlyCleanUpB() throws Exception {
        Database db = new Database(row(101, 1, "9876"));
        db.verify(100, 120);
        assertThat(db.diagnosticSessionAllocated).isFalse();
        assertThat(db.sessionCalls.get(0)).startsWith("B:BEGIN DBMS_LOGMNR.ADD_LOGFILE");
        assertThat(db.sessionCalls.get(db.sessionCalls.size() - 1)).isEqualTo("B:BEGIN DBMS_LOGMNR.END_LOGMNR; END;");
        assertThat(db.sessionCalls.stream().anyMatch(call -> call.startsWith("A:") && call.contains("OPTIONS => 0"))).isFalse();
        assertThat(db.sessionCalls.stream().anyMatch(call -> call.startsWith("B:") && call.contains("DICT_FROM_ONLINE_CATALOG"))).isFalse();
        verify(db.connection, never()).close();
    }

    @Test
    public void shouldRejectSameJdbcConnection() throws Exception {
        Database db = new Database();
        try {
            new LogMinerDictionaryRecovery(db.connection, db.connection, db.config, () -> true);
            fail("Expected separate-connection requirement");
        }
        catch (IllegalArgumentException expected) {
            assertThat(expected.getMessage()).contains("separate JDBC connection");
        }
    }

    @Test
    public void shouldRejectOtherDatabaseBeforeStartingMining() throws Exception {
        Database db = new Database();
        db.diagnosticDbid = "456";
        expectUnsafe(db, "same database incarnation");
        assertThat(db.starts).isEmpty();
        assertThat(db.sessionCalls).isEmpty();
    }

    @Test
    public void shouldCleanUpDiagnosticSessionOnFileMismatchWithoutRestartingA() throws Exception {
        Database db = new Database();
        db.mismatchedDiagnosticFiles = true;
        expectUnsafe(db, "file set differs");
        assertThat(db.diagnosticSessionAllocated).isFalse();
        assertThat(db.sessionCalls.stream().anyMatch(call -> call.startsWith("A:"))).isFalse();
    }

    @Test
    public void shouldCleanUpDiagnosticSessionOnUnsafeMixedScnWithoutRestartingA() throws Exception {
        Database db = new Database(row(101, 1, "9876"), row(101, 3, "999"));
        expectUnsafe(db, "unmapped or ambiguous");
        assertThat(db.diagnosticSessionAllocated).isFalse();
        assertThat(db.sessionCalls.stream().anyMatch(call -> call.startsWith("A:"))).isFalse();
    }

    @Test
    public void shouldNotReturnBoundaryPlanWhenDiagnosticCleanupFails() throws Exception {
        Database db = new Database(row(101, 1, "9876"), row(101, 7, null));
        db.failDiagnosticEnd = true;
        try {
            db.verify(100, 120);
            fail("Expected diagnostic cleanup failure");
        }
        catch (SQLException expected) {
            assertThat(expected.getMessage()).contains("diagnostic end failed");
        }
    }

    @Test
    public void shouldAuditOnlyConfirmedSysRowsAndPreserveBoundaryMetadata() throws Exception {
        Database db = new Database(row(101, 6, null), row(101, 1, "9876"), row(101, 7, null));
        LogMinerDictionaryRecovery.Plan plan = db.verify(100, 120);
        assertThat(plan.audit.summary).contains("discardedSysDml=1").contains("preservedBoundaries=1").contains("anomalyScn=101");
        assertThat(plan.audit.records).hasSize(3);
        assertThat(plan.audit.records.get(1)).contains("DISCARD_CONFIRMED_SYS_DML").contains("SYS.SMON_SCN_TIME").contains("redoThread=1");
        assertThat(plan.audit.records.get(2)).contains("PRESERVE_TRANSACTION_BOUNDARY").contains("xid=abcdef");
        assertThat(plan.boundaries).hasSize(1);
    }

    private static void expectUnsafe(Database db, String reason) throws Exception {
        try {
            db.verify(100, 120);
            fail("Expected rejection: " + reason);
        }
        catch (DebeziumException e) {
            assertThat(e.getMessage()).contains(reason);
        }
    }

    static Map<Object, Object> row(long scn, int code, String objectId) {
        Map<Object, Object> row = new HashMap<>();
        row.put("SCN", Long.toString(scn));
        row.put("OPERATION_CODE", code);
        row.put("DATA_OBJ#", objectId);
        row.put("XID_HEX", "ABCDEF");
        row.put("RS_ID", "0x0001.0002.0003");
        row.put("SSN", 1);
        row.put("STATUS", code == 1 ? 2 : 0);
        row.put("INFO", code == 1 ? "Dictionary Mismatch" : null);
        row.put("CHANGE_TIME", Timestamp.valueOf("2026-09-08 01:00:00"));
        row.put("REDO_THREAD", 1);
        return row;
    }

    static Map<Object, Object> object(int container, String owner, String name) {
        return object(container, owner, name, null, null, name != null && name.endsWith("IDX") ? "INDEX" : "TABLE");
    }

    static Map<Object, Object> object(int container, String owner, String name, String objectId, String dataObjectId, String objectType) {
        Map<Object, Object> row = new HashMap<>();
        row.put(1, Integer.toString(container));
        row.put(2, owner);
        row.put(3, name);
        row.put(4, objectId);
        row.put(5, dataObjectId);
        row.put(6, objectType);
        return row;
    }

    static Map<Object, Object> logFile(String name, Integer status, long low, long next, int thread, long sequence) {
        Map<Object, Object> row = new HashMap<>();
        row.put("FILENAME", name);
        row.put("STATUS", status);
        row.put("INFO", status != null && status == 4 ? "MISSING_LOGFILE" : null);
        row.put("LOW_SCN", Long.toString(low));
        row.put("NEXT_SCN", Long.toString(next));
        row.put("THREAD_ID", thread);
        row.put("THREAD_SQN", Long.toString(sequence));
        row.put("TYPE", name != null && name.startsWith("online") ? "ONLINE" : "ARCHIVED");
        return row;
    }

    /** SQL-aware fixture: restarts retain their actual bounds and all scans use bound predicates. */
    static class Database {
        final Connection connection = mock(Connection.class);
        final Connection diagnosticConnection = mock(Connection.class);
        final OracleConnectorConfig config = mock(OracleConnectorConfig.class);
        final List<Map<Object, Object>> rows;
        final Map<String, List<Map<Object, Object>>> objects = new HashMap<>();
        final List<String> starts = new ArrayList<>();
        final List<String> addedFiles = new ArrayList<>();
        final List<String> countModes = new ArrayList<>();
        List<Map<Object, Object>> logFiles = Arrays.asList(logFile("archive-a", 0, 90, 110, 1, 1), logFile("archive-b", 0, 110, 130, 1, 2));
        List<Map<Object, Object>> diagnosticFiles;
        List<String> registeredOnlineFiles;
        final List<String> registeredDiagnosticFiles = new ArrayList<>();
        boolean diagnosticSessionAllocated;
        boolean failDiagnosticEnd;
        boolean mismatchedDiagnosticFiles;
        String diagnosticDbid = "123";
        String knownObjectId = "9876";
        final List<String> sessionCalls = new ArrayList<>();
        final List<String> sql = new ArrayList<>();
        boolean failOnlineCount;
        boolean failRestore;
        boolean cdb = true;
        String diagnosticContainerId = "1";
        String diagnosticContainerName = "CDB$ROOT";
        boolean failRootDictionary;
        final List<String> dictionaryQueries = new ArrayList<>();
        int countAdjustment;

        @SafeVarargs
        Database(Map<Object, Object>... rows) throws Exception {
            this.rows = Arrays.asList(rows);
            when(config.getCatalogName()).thenReturn("ORCL");
            when(config.getLogMiningViewFetchSize()).thenReturn(1000);
            objects.put("9876", Collections.singletonList(object(1, "SYS", "SMON_SCN_TIME")));
            when(connection.prepareStatement(anyString())).thenAnswer(invocation -> query(invocation.getArgument(0), false));
            when(connection.prepareCall(anyString())).thenAnswer(invocation -> call(invocation.getArgument(0), false));
            when(diagnosticConnection.prepareStatement(anyString())).thenAnswer(invocation -> query(invocation.getArgument(0), true));
            when(diagnosticConnection.prepareCall(anyString())).thenAnswer(invocation -> call(invocation.getArgument(0), true));
        }

        LogMinerDictionaryRecovery.Plan verify(long start, long end) throws Exception {
            try (LogMinerDictionaryRecovery recovery = new LogMinerDictionaryRecovery(connection, diagnosticConnection, config, () -> true)) {
                return recovery.verify(Scn.valueOf(start), Scn.valueOf(end));
            }
        }

        PreparedStatement query(String sql, boolean diagnostic) {
            this.sql.add(sql);
            Map<Integer, String> args = new HashMap<>();
            return mock(PreparedStatement.class, invocation -> {
                String method = invocation.getMethod().getName();
                if (method.equals("setString")) {
                    args.put(invocation.getArgument(0), invocation.getArgument(1));
                }
                if (method.equals("setInt")) {
                    args.put(invocation.getArgument(0), Integer.toString(invocation.getArgument(1)));
                }
                if (method.equals("executeQuery")) {
                    if (sql.contains("FROM V$LOGMNR_LOGS")) {
                        if (diagnostic && !diagnosticSessionAllocated) {
                            throw new AssertionError("B must load the file set from A before inspecting its own logs");
                        }
                        if (diagnostic && mismatchedDiagnosticFiles) {
                            return result(Collections.singletonList(logFile("wrong-archive", 0, 90, 130, 1, 1)));
                        }
                        if (diagnostic && diagnosticFiles != null) {
                            return result(diagnosticFiles);
                        }
                        List<String> registered = diagnostic ? registeredDiagnosticFiles : registeredOnlineFiles;
                        return result(logFiles.stream().filter(file -> registered == null || registered.contains(file.get("FILENAME"))).collect(Collectors.toList()));
                    }
                    if (sql.contains("SELECT DBID")) {
                        return result(Collections.singletonList(values(diagnostic ? diagnosticDbid : "123", "1000", cdb ? "1" : "0", "ORCL", diagnostic ? "20" : "10")));
                    }
                    if (sql.contains("FROM V$DATABASE")) {
                        return result(Collections.singletonList(values(cdb ? "YES" : "NO")));
                    }
                    if (sql.contains("SELECT DATA_OBJECT_ID")) {
                        return result(Collections.singletonList(values(knownObjectId)));
                    }
                    if (sql.contains("FROM DUAL")) {
                        return result(Collections.singletonList(values(cdb ? diagnosticContainerId : "0",
                                cdb ? diagnosticContainerName : "ORCL", "C##XHKJ", "ORCL", diagnostic ? "20" : "10")));
                    }
                    if (sql.contains("FROM DBA_OBJECTS o LEFT JOIN DBA_TABLES")) {
                        if (!diagnostic) {
                            throw new AssertionError("Root dictionary must be read on B");
                        }
                        if (failRootDictionary) {
                            throw new SQLException("root dictionary privilege missing", "42000", 942);
                        }
                        String reference = "1".equals(args.get(2)) ? "INTERNAL_OBJECT_OR_DATA#=" : "DATA_OBJECT#=";
                        dictionaryQueries.add((cdb ? diagnosticContainerId : "0") + ":" + reference + args.get(1));
                        List<Map<Object, Object>> matches = new ArrayList<>();
                        for (Map<Object, Object> stored : objects.getOrDefault(args.get(1), Collections.emptyList())) {
                            if (!(cdb ? diagnosticContainerId : "0").equals(stored.get(1))) {
                                continue;
                            }
                            Map<Object, Object> candidate = new HashMap<>(stored);
                            String objectId = candidate.get(4) == null ? args.get(1) : candidate.get(4).toString();
                            String dataObjectId = candidate.get(5) == null ? args.get(1) : candidate.get(5).toString();
                            if (!args.get(1).equals(dataObjectId) && !("1".equals(args.get(2)) && args.get(1).equals(objectId))) {
                                continue;
                            }
                            candidate.put(4, objectId);
                            candidate.put(5, dataObjectId);
                            matches.add(candidate);
                        }
                        return result(matches);
                    }
                    if (sql.contains("AS ROW_SEQUENCE")) {
                        if (!diagnostic) {
                            throw new AssertionError("Raw metadata must be read on B");
                        }
                        return result(inRange(args.get(1), args.get(2)));
                    }
                    throw new AssertionError("Unexpected query: " + sql);
                }
                return org.mockito.Answers.RETURNS_DEFAULTS.answer(invocation);
            });
        }

        CallableStatement call(String sql, boolean diagnostic) {
            Map<Integer, String> args = new HashMap<>();
            return mock(CallableStatement.class, invocation -> {
                String method = invocation.getMethod().getName();
                if (method.equals("setString")) {
                    args.put(invocation.getArgument(0), invocation.getArgument(1));
                }
                if (method.equals("execute")) {
                    sessionCalls.add((diagnostic ? "B:" : "A:") + sql);
                    if (sql.contains("END_LOGMNR") && diagnostic) {
                        if (failDiagnosticEnd) {
                            throw new SQLException("diagnostic end failed");
                        }
                        if (!diagnosticSessionAllocated) {
                            throw new SQLException("No LogMiner session", "", 1307);
                        }
                        diagnosticSessionAllocated = false;
                    }
                    if (sql.contains("ADD_LOGFILE")) {
                        addedFiles.add(args.get(1));
                        if (sql.contains("DBMS_LOGMNR.NEW")) {
                            if (diagnostic) {
                                registeredDiagnosticFiles.clear();
                            }
                            else {
                                registeredOnlineFiles = new ArrayList<>();
                            }
                        }
                        (diagnostic ? registeredDiagnosticFiles : registeredOnlineFiles).add(args.get(1));
                        if (diagnostic) {
                            diagnosticSessionAllocated = true;
                        }
                    }
                    if (sql.contains("START_LOGMNR")) {
                        boolean online = sql.contains("DICT_FROM_ONLINE_CATALOG");
                        if (online == diagnostic) {
                            throw new AssertionError("A must stay online; B must stay in raw mode");
                        }
                        starts.add((online ? "online" : "raw") + ":" + args.get(1) + ":" + args.get(2));
                        if (online && failRestore) {
                            throw new SQLException("restore failed");
                        }
                    }
                    if (sql.contains("COUNT(*)")) {
                        countModes.add(diagnostic ? "raw" : "online");
                        if (!diagnostic && failOnlineCount) {
                            throw new SQLException("NO_DATA_FOUND during completeness check", "", 20004);
                        }
                    }
                    return false;
                }
                if (method.equals("getBigDecimal")) {
                    return BigDecimal.valueOf(inRange(args.get(2), args.get(3)).size() + countAdjustment);
                }
                return org.mockito.Answers.RETURNS_DEFAULTS.answer(invocation);
            });
        }

        private List<Map<Object, Object>> inRange(String start, String end) {
            return rows.stream().filter(row -> Long.parseLong((String) row.get("SCN")) > Long.parseLong(start)
                    && Long.parseLong((String) row.get("SCN")) <= Long.parseLong(end)).collect(Collectors.toList());
        }

        static Map<Object, Object> values(Object... values) {
            Map<Object, Object> row = new HashMap<>();
            for (int i = 0; i < values.length; i++) {
                row.put(i + 1, values[i]);
            }
            return row;
        }

        static ResultSet result(List<Map<Object, Object>> rows) {
            AtomicInteger index = new AtomicInteger(-1);
            AtomicBoolean wasNull = new AtomicBoolean();
            Answer<Object> answer = invocation -> {
                String method = invocation.getMethod().getName();
                if (method.equals("next")) {
                    return index.incrementAndGet() < rows.size();
                }
                if (method.equals("getString")) {
                    Object value = rows.get(index.get()).get(invocation.getArgument(0));
                    wasNull.set(value == null);
                    return value == null ? null : value.toString();
                }
                if (method.equals("getInt")) {
                    Object value = rows.get(index.get()).get(invocation.getArgument(0));
                    wasNull.set(value == null);
                    return value == null ? 0 : Integer.parseInt(value.toString());
                }
                if (method.equals("getTimestamp")) {
                    return rows.get(index.get()).get(invocation.getArgument(0));
                }
                if (method.equals("wasNull")) {
                    return wasNull.get();
                }
                return org.mockito.Answers.RETURNS_DEFAULTS.answer(invocation);
            };
            return mock(ResultSet.class, answer);
        }
    }
}
