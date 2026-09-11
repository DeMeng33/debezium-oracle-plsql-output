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
import java.util.HashSet;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.junit.Test;
import org.mockito.stubbing.Answer;

import io.debezium.DebeziumException;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.logminer.events.EventType;
import io.debezium.relational.RelationalTableFilters;
import io.debezium.relational.Tables.TableFilter;
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
    public void shouldVerifyDeployedObjectlessInternalWithSameSysTransaction() throws Exception {
        Database db = new Database(row(110, 0, "0"), row(111, 0, "347"), row(111, 0, "346"));
        db.objects.put("347", Collections.singletonList(object(1, "SYS", "SMON_SCN_TIME", "347", "345", "TABLE")));
        db.objects.put("346", Collections.singletonList(object(1, "SYS", "SMON_SCN_TO_TIME_AUX_IDX", "346", "346", "INDEX")));
        LogInterceptor logs = new LogInterceptor(LogMinerDictionaryRecovery.class);
        try {
            LogMinerDictionaryRecovery.Plan plan = db.verify(100, 120);
            assertThat(plan.replayOnline).isTrue();
            assertThat(plan.emptyReplayRejection).isNull();
            assertThat(db.dictionaryQueries).hasSize(2).contains("1:OBJECT_ID_OR_DATA#=347", "1:OBJECT_ID_OR_DATA#=346");
            assertThat(db.dictionaryQueries.stream().anyMatch(query -> query.endsWith("#=0"))).isFalse();
            assertThat(logs.containsMessage("NOT_APPLICABLE:INTERNAL_WITHOUT_OBJECT_REFERENCE")).isTrue();
            assertThat(logs.containsMessage("associated with SYS transaction in filtered interval")).isTrue();
            assertThat(logs.containsMessage("CTXSYS.DR$DBO")).isFalse();
        }
        finally {
            logs.stop();
        }
    }

    @Test
    public void shouldRejectObjectlessInternalWithoutSameSysTransaction() throws Exception {
        Database isolated = new Database(row(110, 0, "0"));
        LogMinerDictionaryRecovery.Plan isolatedPlan = isolated.verify(100, 120);
        assertThat(isolatedPlan.emptyReplayRejection).contains("objectless INTERNAL without a verified SYS transaction association");
        assertThat(isolated.dictionaryQueries).isEmpty();

        Map<Object, Object> objectless = row(110, 0, "0");
        objectless.put("XID_HEX", "OTHER");
        Database differentTransaction = new Database(objectless, row(111, 0, "347"));
        differentTransaction.objects.put("347", Collections.singletonList(object(1, "SYS", "SMON_SCN_TIME", "347", "345", "TABLE")));
        assertThat(differentTransaction.verify(100, 120).emptyReplayRejection)
                .contains("objectless INTERNAL without a verified SYS transaction association");

        Map<Object, Object> invalidDuplicate = row(112, 0, "0");
        invalidDuplicate.put("STATUS", 2);
        Database duplicate = new Database(row(110, 0, "0"), invalidDuplicate, row(111, 0, "347"));
        duplicate.objects.put("347", Collections.singletonList(object(1, "SYS", "SMON_SCN_TIME", "347", "345", "TABLE")));
        assertThat(duplicate.verify(100, 120).emptyReplayRejection)
                .contains("objectless INTERNAL without a verified SYS transaction association").contains("status=2");
    }

    @Test
    public void shouldFilterConfirmedNonCapturedDmlUsingObjectIdFallback() throws Exception {
        Database db = new Database(row(101, 1, "157440"));
        db.excludedTables.add("T_CAR_INFO_OUT_LAST");
        db.objects.put("157440", Collections.singletonList(object(1, "C##XHKJ", "T_CAR_INFO_OUT_LAST", "157440", "181012", "TABLE")));
        LogInterceptor logs = new LogInterceptor(LogMinerDictionaryRecovery.class);
        try {
            LogMinerDictionaryRecovery.Plan plan = db.verify(100, 120);
            assertThat(plan.replayOnline).isTrue();
            assertThat(plan.emptyReplayRejection).isNull();
            assertThat(db.dictionaryQueries).containsExactly("1:OBJECT_ID_OR_DATA#=157440");
            assertThat(logs.containsMessage("captureDecision=EXCLUDED_NON_CAPTURED_TABLE")).isTrue();
            assertThat(logs.containsErrorMessage("LOGMINER_DIAGNOSIS_FAILED")).isFalse();
        }
        finally {
            logs.stop();
        }
    }

    @Test
    public void shouldKeepNonCapturedBoundaryDmlOutOfErrorAudit() throws Exception {
        Database db = new Database(row(101, 1, "9876"), row(101, 1, "157440"));
        db.excludedTables.add("T_CAR_INFO_OUT_LAST");
        db.objects.put("157440", Collections.singletonList(object(1, "C##XHKJ", "T_CAR_INFO_OUT_LAST", "157440", "181012", "TABLE")));
        LogInterceptor logs = new LogInterceptor(LogMinerDictionaryRecovery.class);
        try {
            LogMinerDictionaryRecovery.Plan plan = db.verify(100, 120);
            assertThat(plan.audit).isNotNull();
            assertThat(plan.audit.records.toString()).doesNotContain("T_CAR_INFO_OUT_LAST");
            assertThat(plan.audit.summary).contains("discardedSysDml=1");
            assertThat(logs.containsMessage("LOGMINER_NON_CAPTURED_DML_FILTERED")).isTrue();
            assertThat(logs.containsErrorMessage("T_CAR_INFO_OUT_LAST")).isFalse();
        }
        finally {
            logs.stop();
        }
    }

    @Test
    public void shouldAcceptObjectlessInternalAtSysBoundaryOnlyWithSameTransaction() throws Exception {
        Database db = new Database(row(101, 1, "9876"), row(101, 0, "0"));
        LogMinerDictionaryRecovery.Plan plan = db.verify(100, 120);
        assertThat(plan.end).isEqualTo(Scn.valueOf(101));
        assertThat(plan.audit.records.get(1)).contains("IGNORE_NON_CAPTURED_OPERATION")
                .contains("NOT_APPLICABLE:INTERNAL_WITHOUT_OBJECT_REFERENCE");
    }

    @Test
    public void shouldNotAdvanceUnknownOnlineDictionaryFailure() throws Exception {
        Database db = new Database(row(110, 1, "999"));
        db.failOnlineCount = true;
        try {
            db.verify(100, 120);
            fail("Expected unresolved raw object rejection");
        }
        catch (DebeziumException e) {
            assertThat(e.getMessage()).contains("unmapped or ambiguous");
            assertThat(e.getSuppressed()).hasSize(1);
            assertThat(((SQLException) e.getSuppressed()[0]).getErrorCode()).isEqualTo(20004);
        }
    }

    @Test
    public void shouldRecoverVerifiedMetadataAcrossDeployedRangeWhenOnlineCountFails() throws Exception {
        // The production log supplies the range and count, not all 756 row contents.
        // Use synthetic, explicitly mapped metadata to verify the conditional recovery.
        List<Map<Object, Object>> rows = new ArrayList<>();
        rows.add(row(235476226, 6, null));
        rows.add(selectForUpdate(235476226));
        for (int i = 0; i < 752; i++) {
            Map<Object, Object> internal = row(235476227 + i / 44, 0, "344");
            internal.put("SSN", i);
            rows.add(internal);
        }
        rows.add(row(235476244, 7, null));
        rows.add(row(235476244, 36, null));
        Database db = new Database(rows.toArray(new Map[0]));
        db.failOnlineCount = true;
        db.objects.put("344", Collections.singletonList(object(1, "SYS", "PENDING_SUB_SESSIONS$")));
        db.logFiles = Collections.singletonList(logFile("archive-36846", 0, 235476225, 235496112, 1, 36846));
        LogMinerDictionaryRecovery.Plan plan = db.verify(235476225, 235476244);
        assertThat(plan.end).isEqualTo(Scn.valueOf(235476244));
        assertThat(plan.replayOnline).isFalse();
        assertThat(plan.boundaries).hasSize(2);
        assertThat(plan.boundaries.get(0).getEventType()).isEqualTo(EventType.COMMIT);
        assertThat(plan.boundaries.get(1).getEventType()).isEqualTo(EventType.ROLLBACK);
        assertThat(plan.audit.summary).contains("verificationMode=RAW_FILTERED_WINDOW").contains("rawWindowRows=756");
        assertThat(db.starts).containsExactly("raw:235476226:235476244", "online:235476226:235476244", "online:235476226:235476244");
        assertThat(db.diagnosticSessionAllocated).isFalse();
    }

    @Test
    public void shouldFilterExcludedTableDmlWhenOnlineCountFails() throws Exception {
        Database db = new Database(row(110, 1, "157440"), row(111, 7, null));
        db.failOnlineCount = true;
        db.excludedTables.add("T_CAR_INFO_OUT_LAST");
        db.objects.put("157440", Collections.singletonList(object(1, "C##XHKJ", "T_CAR_INFO_OUT_LAST", "157440", "181012", "TABLE")));
        LogMinerDictionaryRecovery.Plan plan = db.verify(100, 120);
        assertThat(plan.boundaries).hasSize(1);
        assertThat(plan.audit.records.toString()).doesNotContain("T_CAR_INFO_OUT_LAST");
        assertThat(plan.audit.summary).contains("discardedSysDml=0");
    }

    @Test
    public void shouldRejectCapturedDmlAnywhereInRawFilteredWindow() throws Exception {
        for (int code : Arrays.asList(1, 2, 3)) {
            Database db = new Database(row(101, 0, "344"), row(102, 7, null), row(119, code, "999"));
            db.failOnlineCount = true;
            db.objects.put("344", Collections.singletonList(object(1, "SYS", "PENDING_SUB_SESSIONS$")));
            db.objects.put("999", Collections.singletonList(object(1, "APP", "CAPTURED_TABLE")));
            expectUnsafe(db, "captured or unsupported object");
            assertThat(db.starts).containsExactly("raw:101:120", "online:101:120");
        }
    }

    @Test
    public void shouldKeepRejectingUnsupportedOperationsWhenOnlineCountFails() throws Exception {
        for (int code : Arrays.asList(5, 9, 10, 11, 29, 68, 70, 71, 255, 99)) {
            Database db = new Database(row(101, 6, null), row(102, 7, null), row(119, code, "95420"));
            db.failOnlineCount = true;
            expectUnsafe(db, "unsupported operation");
        }
        Database sysDml = new Database(row(110, 1, "344"));
        sysDml.failOnlineCount = true;
        sysDml.objects.put("344", Collections.singletonList(object(1, "SYS", "PENDING_SUB_SESSIONS$")));
        expectUnsafe(sysDml, "captured or unsupported object");
    }

    @Test
    public void shouldValidateEveryRawFilteredRowAfterOnlineCountFailure() throws Exception {
        for (int code : Arrays.asList(0, 1, 6, 7, 25, 36)) {
            for (String field : Arrays.asList("XID_HEX", "CHANGE_TIME", "RS_ID", "SSN", "REDO_THREAD", "CSF", "STATUS")) {
                Map<Object, Object> invalid = row(110, code, "999");
                invalid.put(field, null);
                Database db = new Database(row(110, code, "999"), invalid);
                db.failOnlineCount = true;
                db.excludedTables.add("EXCLUDED_TABLE");
                db.objects.put("999", Collections.singletonList(object(1, code == 0 ? "SYS" : "APP", "EXCLUDED_TABLE")));
                expectUnsafe(db, "incomplete or invalid raw filtered-window metadata");
            }
        }
        Map<Object, Object> invalidCommit = row(110, 7, null);
        invalidCommit.put("STATUS", 2);
        Database db = new Database(invalidCommit);
        db.failOnlineCount = true;
        expectUnsafe(db, "incomplete transaction boundary");
    }

    @Test
    public void shouldRequireSysAssociationForObjectlessRawFilteredInternal() throws Exception {
        Database db = new Database(row(110, 0, "0"), row(111, 0, "344"));
        db.failOnlineCount = true;
        db.objects.put("344", Collections.singletonList(object(1, "SYS", "PENDING_SUB_SESSIONS$")));
        assertThat(db.verify(100, 120).end).isEqualTo(Scn.valueOf(120));
        Database isolated = new Database(row(110, 0, "0"));
        isolated.failOnlineCount = true;
        expectUnsafe(isolated, "without a verified SYS transaction association");
    }

    @Test
    public void shouldRejectIncompleteSecondRawScanAfterOnlineCountFailure() throws Exception {
        Database db = new Database(row(110, 6, null), row(111, 7, null));
        db.failOnlineCount = true;
        db.truncateSecondRawScan = true;
        expectUnsafe(db, "Incomplete metadata range");
        assertThat(db.dictionaryQueries).isEmpty();
    }

    @Test
    public void shouldNotRecoverUnrelatedOnlineCountErrorOrSuccessfulCountMismatch() throws Exception {
        Database db = new Database(row(110, 6, null));
        db.failOnlineCount = true;
        db.onlineCountErrorCode = 942;
        try {
            db.verify(100, 120);
            fail("Expected original SQL failure");
        }
        catch (SQLException e) {
            assertThat(e.getErrorCode()).isEqualTo(942);
        }
        assertThat(db.rawScans).isEqualTo(1);
        db = new Database(row(110, 6, null));
        db.onlineCountAdjustment = 1;
        expectUnsafe(db, "Online/raw LogMiner row count mismatch");
        assertThat(db.rawScans).isEqualTo(1);
    }

    @Test
    public void shouldKeepLobReminingDisabledAfterOnlineCountFailure() throws Exception {
        Database db = new Database(row(110, 6, null));
        db.failOnlineCount = true;
        when(db.config.isLobEnabled()).thenReturn(true);
        expectUnsafe(db, "does not support LOB");
    }

    @Test
    public void shouldFilterIndexInternalByExcludedParentTableInAllRecoveryPaths() throws Exception {
        for (int mode : Arrays.asList(0, 1, 2)) {
            Database db = mode == 0 ? new Database(row(101, 1, "9876"), row(101, 0, "158559"), row(101, 7, null))
                    : new Database(row(101, 0, "158559"));
            db.failOnlineCount = mode == 2;
            excludedIndex(db, "158559", "OUT_LAST_CAR_ID_IDX", "T_CAR_INFO_OUT_LAST");
            LogInterceptor logs = new LogInterceptor(LogMinerDictionaryRecovery.class);
            try {
                LogMinerDictionaryRecovery.Plan plan = db.verify(100, 120);
                assertThat(plan.emptyReplayRejection).isNull();
                assertThat(plan.replayOnline).isEqualTo(mode == 1);
                assertThat(plan.end).isEqualTo(Scn.valueOf(mode == 0 ? 101 : 120));
                assertThat(plan.boundaries).hasSize(mode == 0 ? 1 : 0);
                assertThat(db.indexQueries).containsExactly("C##XHKJ.OUT_LAST_CAR_ID_IDX");
                assertThat(logs.containsMessage("LOGMINER_NON_CAPTURED_INDEX_FILTERED")).isTrue();
                if (plan.audit != null) {
                    assertThat(plan.audit.records.toString()).doesNotContain("OUT_LAST_CAR_ID_IDX");
                }
            }
            finally {
                logs.stop();
            }
        }
    }

    @Test
    public void shouldNeverUseIndexNameAsTableFilterOrInferMissingParent() throws Exception {
        for (boolean missingParent : Arrays.asList(false, true)) {
            Database db = new Database(row(101, 1, "9876"), row(101, 0, "95991"));
            excludedIndex(db, "95991", "IDX_QRTZ_FT_T_G", "QRTZ_FIRED_TRIGGERS");
            db.excludedTables.clear();
            db.excludedTables.add("IDX_QRTZ_FT_T_G");
            if (missingParent) {
                db.indexes.clear();
            }
            expectUnsafe(db, "captured or unsupported object");
        }
    }

    @Test
    public void shouldRejectAmbiguousIndexParentAndSpecialIndexTypes() throws Exception {
        for (String type : Arrays.asList("DOMAIN", "IOT - TOP", "LOB", "CLUSTER", "BITMAP", "FUNCTION-BASED NORMAL")) {
            Database db = new Database(row(101, 1, "9876"), row(101, 0, "95991"));
            excludedIndex(db, "95991", "IDX_QRTZ_FT_T_G", "QRTZ_FIRED_TRIGGERS");
            db.indexes.put("C##XHKJ.IDX_QRTZ_FT_T_G", Collections.singletonList(Database.values("C##XHKJ", "QRTZ_FIRED_TRIGGERS", type, "TABLE")));
            expectUnsafe(db, "captured or unsupported object");
        }
        Database db = new Database(row(101, 1, "9876"), row(101, 0, "95991"));
        excludedIndex(db, "95991", "IDX_QRTZ_FT_T_G", "QRTZ_FIRED_TRIGGERS");
        db.indexes.put("C##XHKJ.IDX_QRTZ_FT_T_G", Arrays.asList(
                Database.values("C##XHKJ", "QRTZ_FIRED_TRIGGERS", "NORMAL", "TABLE"),
                Database.values("APP", "CAPTURED_TABLE", "NORMAL", "TABLE")));
        expectUnsafe(db, "captured or unsupported object");
    }

    @Test
    public void shouldKeepPhysicalIndexIdentityUnique() throws Exception {
        Database db = new Database(row(101, 1, "9876"), row(101, 0, "95991"));
        excludedIndex(db, "95991", "IDX_QRTZ_FT_T_G", "QRTZ_FIRED_TRIGGERS");
        db.objects.put("95991", Arrays.asList(
                object(1, "C##XHKJ", "IDX_QRTZ_FT_T_G", "95991", "95991", "INDEX"),
                object(1, "C##XHKJ", "IDX_QRTZ_FT_T_G", "95992", "95991", "TABLE")));
        expectUnsafe(db, "captured or unsupported object");
        assertThat(db.indexQueries).isEmpty();
    }

    @Test
    public void shouldFilterOrdinaryIndexPartitionsButNeverDmlOnIndexReference() throws Exception {
        for (String type : Arrays.asList("INDEX", "INDEX PARTITION", "INDEX SUBPARTITION")) {
            Database db = new Database(row(101, 1, "9876"), row(101, 0, "158559"));
            excludedIndex(db, "158559", "OUT_LAST_CAR_ID_IDX", "T_CAR_INFO_OUT_LAST");
            db.objects.put("158559", Collections.singletonList(object(1, "C##XHKJ", "OUT_LAST_CAR_ID_IDX", "158559", "181010", type)));
            assertThat(db.verify(100, 120).end).isEqualTo(Scn.valueOf(101));
        }
        Database dml = new Database(row(101, 1, "9876"), row(101, 1, "158559"));
        excludedIndex(dml, "158559", "OUT_LAST_CAR_ID_IDX", "T_CAR_INFO_OUT_LAST");
        expectUnsafe(dml, "OBJECT_ID fallback is only allowed for an excluded non-system table");
        assertThat(dml.indexQueries).isEmpty();
    }

    @Test
    public void shouldValidateEveryExcludedIndexInternalEvenWhenObjectsAreDeduplicated() throws Exception {
        for (String field : Arrays.asList("CSF", "SSN", "XID_HEX", "REDO_THREAD", "CHANGE_TIME", "RS_ID")) {
            Map<Object, Object> invalid = row(101, 0, "158559");
            invalid.put(field, null);
            Database db = new Database(row(101, 0, "158559"), invalid);
            excludedIndex(db, "158559", "OUT_LAST_CAR_ID_IDX", "T_CAR_INFO_OUT_LAST");
            assertThat(db.verify(100, 120).emptyReplayRejection).contains("incomplete excluded-index INTERNAL");
            Database boundary = new Database(row(101, 1, "9876"), row(101, 0, "158559"), invalid);
            excludedIndex(boundary, "158559", "OUT_LAST_CAR_ID_IDX", "T_CAR_INFO_OUT_LAST");
            expectUnsafe(boundary, "incomplete excluded-index INTERNAL");
        }
    }

    @Test
    public void shouldFilterVerifiedRootBootstrapInternalWithoutDictionaryLookup() throws Exception {
        for (boolean onlineCountUnavailable : Arrays.asList(false, true)) {
            Database db = new Database(row(101, 0, "1"));
            db.failOnlineCount = onlineCountUnavailable;
            LogInterceptor logs = new LogInterceptor(LogMinerDictionaryRecovery.class);
            try {
                LogMinerDictionaryRecovery.Plan plan = db.verify(100, 120);
                assertThat(plan.end).isEqualTo(Scn.valueOf(120));
                assertThat(plan.replayOnline).isEqualTo(!onlineCountUnavailable);
                assertThat(plan.emptyReplayRejection).isNull();
                assertThat(db.dictionaryQueries).isEmpty();
                assertThat(logs.containsMessage("LOGMINER_ROOT_BOOTSTRAP_INTERNAL_FILTERED")).isTrue();
            }
            finally {
                logs.stop();
            }
        }
    }

    @Test
    public void shouldRejectBootstrapObjectOneOutsideStrictInternalMetadataShape() throws Exception {
        Database dml = new Database(row(101, 1, "1"));
        dml.failOnlineCount = true;
        expectUnsafe(dml, "unmapped or ambiguous DATA_OBJECT#=");

        Map<Object, Object> statusTwo = row(101, 0, "1");
        statusTwo.put("STATUS", 2);
        Database invalidStatus = new Database(statusTwo);
        invalidStatus.failOnlineCount = true;
        expectUnsafe(invalidStatus, "incomplete root bootstrap INTERNAL metadata");

        Map<Object, Object> missingRsId = row(101, 0, "1");
        missingRsId.put("RS_ID", null);
        Database incomplete = new Database(missingRsId);
        incomplete.failOnlineCount = true;
        expectUnsafe(incomplete, "incomplete or invalid raw filtered-window metadata");
    }

    @Test
    public void shouldFailWhenIndexParentDictionaryCannotBeRead() throws Exception {
        Database db = new Database(row(101, 1, "9876"), row(101, 0, "158559"), row(101, 7, null));
        excludedIndex(db, "158559", "OUT_LAST_CAR_ID_IDX", "T_CAR_INFO_OUT_LAST");
        db.failIndexDictionary = true;
        try {
            db.verify(100, 120);
            fail("Expected index dictionary error");
        }
        catch (SQLException e) {
            assertThat(e.getErrorCode()).isEqualTo(942);
        }
        assertThat(db.starts).containsExactly("raw:101:120");
    }

    static void excludedIndex(Database db, String reference, String indexName, String tableName) {
        db.excludedTables.add(tableName);
        db.objects.put(reference, Collections.singletonList(object(1, "C##XHKJ", indexName, reference, "181010", "INDEX")));
        db.indexes.put("C##XHKJ." + indexName, Collections.singletonList(Database.values("C##XHKJ", tableName, "NORMAL", "TABLE")));
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
    public void shouldRecoverDeployedSelectForUpdateAtSmonBoundary() throws Exception {
        Map<Object, Object> select = selectForUpdate(235476225);
        select.put("XID_HEX", "0b000d0068800400");
        select.put("RS_ID", " 0x008fee.0000000c.0010 ");
        select.put("SSN", 0);
        select.put("CHANGE_TIME", Timestamp.valueOf("2026-09-10 05:54:07"));
        Database db = new Database(row(235476225, 1, "345"), select,
                row(235476225, 7, null), row(235476225, 36, null), row(235476226, 3, "95420"));
        db.knownObjectId = "345";
        db.objects.put("345", Collections.singletonList(object(1, "SYS", "SMON_SCN_TIME", "347", "345", "TABLE")));
        db.logFiles = Collections.singletonList(logFile("archive-36846", 0, 235476225, 235496112, 1, 36846));
        LogInterceptor logs = new LogInterceptor(LogMinerDictionaryRecovery.class);
        try {
            LogMinerDictionaryRecovery.Plan plan = db.verify(235476224, 235496111);
            assertThat(plan.end).isEqualTo(Scn.valueOf(235476225));
            assertThat(plan.replayOnline).isFalse();
            assertThat(plan.boundaries).hasSize(2);
            assertThat(plan.boundaries.get(0).getEventType()).isEqualTo(EventType.COMMIT);
            assertThat(plan.boundaries.get(1).getEventType()).isEqualTo(EventType.ROLLBACK);
            assertThat(plan.audit.summary).contains("discardedSysDml=1").contains("filteredSelectForUpdate=1");
            assertThat(plan.audit.records.toString()).doesNotContain("operationCode=25");
            assertThat(db.dictionaryQueries).containsExactly("1:OBJECT_ID_OR_DATA#=345");
            assertThat(logs.containsMessage("LOGMINER_SELECT_FOR_UPDATE_FILTERED")).isTrue();
        }
        finally {
            logs.stop();
        }
    }

    @Test
    public void shouldAllowFilteredSelectForUpdateWindowForBothRawStatuses() throws Exception {
        for (int status : Arrays.asList(0, 2)) {
            Map<Object, Object> select = selectForUpdate(110);
            select.put("STATUS", status);
            Database db = new Database(select);
            LogMinerDictionaryRecovery.Plan plan = db.verify(100, 120);
            assertThat(plan.replayOnline).isTrue();
            assertThat(plan.emptyReplayRejection).isNull();
            assertThat(plan.boundaries).isEmpty();
            assertThat(db.countModes).containsExactly("raw", "online");
            assertThat(db.dictionaryQueries).isEmpty();
        }
    }

    @Test
    public void shouldRejectEveryIncompleteSelectForUpdateInBothRecoveryPaths() throws Exception {
        Map<String, List<Object>> invalidFields = new LinkedHashMap<>();
        invalidFields.put("STATUS", Arrays.asList(null, 5));
        invalidFields.put("CSF", Arrays.asList(null, 1));
        invalidFields.put("XID_HEX", Arrays.asList(null, ""));
        invalidFields.put("REDO_THREAD", Arrays.asList(null, 0));
        invalidFields.put("CHANGE_TIME", Collections.singletonList(null));
        invalidFields.put("RS_ID", Arrays.asList(null, " "));
        invalidFields.put("SSN", Arrays.asList(null, -1));
        for (Map.Entry<String, List<Object>> field : invalidFields.entrySet()) {
            for (Object value : field.getValue()) {
                // Same SCN, object, XID and record identity: no representative row may hide
                // an incomplete second record, including an unfinished SQL continuation.
                Map<Object, Object> invalid = selectForUpdate(101);
                invalid.put(field.getKey(), value);
                Database boundary = new Database(row(101, 1, "9876"), selectForUpdate(101), invalid);
                expectUnsafe(boundary, "incomplete or invalid SELECT_FOR_UPDATE");
                Database filtered = new Database(selectForUpdate(101), invalid);
                assertThat(filtered.verify(100, 120).emptyReplayRejection).isNotNull();
            }
        }
    }

    @Test
    public void shouldStillRequireOnlineCommitAndRollbackAfterSelectForUpdate() throws Exception {
        for (int code : Arrays.asList(7, 36)) {
            Database db = new Database(selectForUpdate(110), row(110, code, null));
            assertThat(db.verify(100, 120).emptyReplayRejection).contains("raw redo requires an online row");
        }
    }

    @Test
    public void shouldNotFilterCapturedDmlSharingSelectForUpdateObjectAndTransaction() throws Exception {
        for (int code : Arrays.asList(1, 2, 3)) {
            Database db = new Database(row(101, 1, "9876"), selectForUpdate(101), row(101, 7, null), row(101, code, "95420"));
            db.objects.put("95420", Collections.singletonList(object(1, "APP", "CAPTURED_TABLE")));
            expectUnsafe(db, "captured or unsupported object");
            Database filtered = new Database(selectForUpdate(101), row(101, code, "95420"));
            filtered.objects.put("95420", Collections.singletonList(object(1, "APP", "CAPTURED_TABLE")));
            assertThat(filtered.verify(100, 120).emptyReplayRejection).contains("potentially captured or invalid operation");
        }
    }

    @Test
    public void shouldKeepRejectingDdlLobXmlAndUnknownOperationsAlongsideSelectForUpdate() throws Exception {
        for (int code : Arrays.asList(5, 9, 10, 11, 29, 68, 70, 71, 255, 99)) {
            Database db = new Database(row(101, 1, "9876"), selectForUpdate(101), row(101, code, "95420"));
            expectUnsafe(db, "unsupported operation");
            Database filtered = new Database(selectForUpdate(101), row(101, code, "95420"));
            assertThat(filtered.verify(100, 120).emptyReplayRejection).contains("raw redo requires an online row");
        }
        expectUnsafe(new Database(row(101, 1, "9876"), selectForUpdate(101), row(101, 34, null)), "missing redo");
    }

    @Test
    public void shouldNotUseSelectForUpdateAsSysEvidenceForObjectlessInternal() throws Exception {
        Database db = new Database(selectForUpdate(110), row(110, 0, "0"));
        assertThat(db.verify(100, 120).emptyReplayRejection).contains("without a verified SYS transaction association");
    }

    static Map<Object, Object> selectForUpdate(long scn) {
        Map<Object, Object> select = row(scn, 25, "95420");
        select.put("STATUS", 2);
        select.put("INFO", "Dictionary Mismatch");
        return select;
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
        assertThat(db.dictionaryQueries).containsExactly("1:OBJECT_ID_OR_DATA#=9876");
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
        assertThat(db.dictionaryQueries).containsExactly("1:OBJECT_ID_OR_DATA#=345");
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
        assertThat(db.dictionaryQueries).containsExactly("1:OBJECT_ID_OR_DATA#=345", "1:OBJECT_ID_OR_DATA#=346",
                "1:OBJECT_ID_OR_DATA#=347");
        assertThat(plan.audit.summary).contains("INTERNAL_OBJECT_OR_DATA#=347=[1:SYS.SMON_SCN_TIME]")
                .contains("matchedBy=OBJECT_ID_FALLBACK, OBJECT_ID=347, DATA_OBJECT_ID=345, OBJECT_TYPE=TABLE");
        assertThat(plan.audit.records.get(2)).contains("IGNORE_NON_CAPTURED_OPERATION").contains("SYS.SMON_SCN_TIME");
    }

    @Test
    public void shouldNotUseObjectIdFallbackForDml() throws Exception {
        Database db = new Database(row(101, 2, "347"));
        db.knownObjectId = "347";
        db.objects.put("347", Collections.singletonList(object(1, "SYS", "SMON_SCN_TIME", "347", "345", "TABLE")));
        expectUnsafe(db, "OBJECT_ID fallback is only allowed for an excluded non-system table");
        assertThat(db.dictionaryQueries).containsExactly("1:OBJECT_ID_OR_DATA#=347");
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
        assertThat(db.dictionaryQueries).containsExactly("0:OBJECT_ID_OR_DATA#=9876");
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
        assertThat(db.dictionaryQueries).containsExactly("1:OBJECT_ID_OR_DATA#=999");
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
        row.put("CSF", 0);
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
        final Map<String, List<Map<Object, Object>>> indexes = new HashMap<>();
        final List<String> indexQueries = new ArrayList<>();
        boolean failIndexDictionary;
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
        int onlineCountErrorCode = 20004;
        int onlineCountAdjustment;
        int rawScans;
        boolean truncateSecondRawScan;
        boolean failRestoreAfterCount;
        boolean failRestore;
        boolean cdb = true;
        String diagnosticContainerId = "1";
        String diagnosticContainerName = "CDB$ROOT";
        boolean failRootDictionary;
        final List<String> dictionaryQueries = new ArrayList<>();
        final Set<String> excludedTables = new HashSet<>();
        int countAdjustment;

        @SafeVarargs
        Database(Map<Object, Object>... rows) throws Exception {
            this.rows = Arrays.asList(rows);
            when(config.getCatalogName()).thenReturn("ORCL");
            when(config.getLogMiningViewFetchSize()).thenReturn(1000);
            RelationalTableFilters filters = mock(RelationalTableFilters.class);
            TableFilter tableFilter = TableFilter.fromPredicate(tableId -> !excludedTables.contains(tableId.table()));
            when(filters.dataCollectionFilter()).thenReturn(tableFilter);
            when(config.getTableFilters()).thenReturn(filters);
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
                    if (sql.contains("FROM DBA_INDEXES")) {
                        if (!diagnostic) {
                            throw new AssertionError("Index parent dictionary must be read on B");
                        }
                        if (failIndexDictionary) {
                            throw new SQLException("index dictionary privilege missing", "42000", 942);
                        }
                        String index = args.get(1) + "." + args.get(2);
                        indexQueries.add(index);
                        return result(indexes.getOrDefault(index, Collections.emptyList()));
                    }
                    if (sql.contains("FROM DBA_OBJECTS o LEFT JOIN DBA_TABLES")) {
                        if (!diagnostic) {
                            throw new AssertionError("Root dictionary must be read on B");
                        }
                        if (failRootDictionary) {
                            throw new SQLException("root dictionary privilege missing", "42000", 942);
                        }
                        String reference = "OBJECT_ID_OR_DATA#=";
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
                        rawScans++;
                        List<Map<Object, Object>> raw = inRange(args.get(1), args.get(2));
                        if (truncateSecondRawScan && rawScans == 2) {
                            raw = raw.subList(0, raw.size() - 1);
                        }
                        return result(raw);
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
                        if (online && (failRestore || (failRestoreAfterCount && countModes.contains("online")))) {
                            throw new SQLException("restore failed");
                        }
                    }
                    if (sql.contains("COUNT(*)")) {
                        countModes.add(diagnostic ? "raw" : "online");
                        if (!diagnostic && failOnlineCount) {
                            throw new SQLException("NO_DATA_FOUND during completeness check", "", onlineCountErrorCode);
                        }
                    }
                    return false;
                }
                if (method.equals("getBigDecimal")) {
                    return BigDecimal.valueOf(inRange(args.get(2), args.get(3)).size() + countAdjustment + (diagnostic ? 0 : onlineCountAdjustment));
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
