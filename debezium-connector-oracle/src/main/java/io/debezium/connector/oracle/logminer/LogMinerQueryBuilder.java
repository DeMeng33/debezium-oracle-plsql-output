/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleDatabaseSchema;
import io.debezium.connector.oracle.logminer.logwriter.LogWriterFlushStrategy;
import io.debezium.util.Strings;

/**
 * A builder that is responsible for producing the query to be executed against the LogMiner view.
 *
 * @author Chris Cranford
 */
public class LogMinerQueryBuilder {

    private static final String LOGMNR_CONTENTS_VIEW = "V$LOGMNR_CONTENTS";
    private static final String SELECT_LIST = "SELECT SCN, SQL_REDO, OPERATION_CODE, TIMESTAMP, XID, CSF, TABLE_NAME, SEG_OWNER, OPERATION, "
            + "USERNAME, ROW_ID, ROLLBACK, RS_ID, STATUS, INFO, SSN, THREAD# "
            + "FROM " + LOGMNR_CONTENTS_VIEW + " ";
    private static final String PLSQL_OUTPUT_TABLE_NAME = "NVL(TABLE_NAME, SEG_NAME)";
    private static final String PLSQL_OUTPUT_SELECT_LIST = "SELECT SCN, SQL_REDO, OPERATION_CODE, TIMESTAMP AS CHANGE_TIME, LOWER(RAWTOHEX(XID)) AS XID_HEX, "
            + "CSF, " + PLSQL_OUTPUT_TABLE_NAME
            + " AS TABLE_NAME, SEG_OWNER, OPERATION, USERNAME, ROW_ID, ROLLBACK AS ROLLBACK_FLAG, RS_ID, STATUS, INFO, SSN, THREAD# AS THREAD_NUMBER "
            + "FROM " + LOGMNR_CONTENTS_VIEW + " ";
    private static final int PLSQL_OUTPUT_MAX_BYTES = 64 * 1024 * 1024;

    /**
     * Builds the LogMiner contents view query.
     *
     * The returned query will contain 2 bind parameters that the caller is responsible for binding before
     * executing the query.  The first bind parameter is the lower-bounds of the SCN mining window that is
     * not-inclusive while the second is the upper-bounds of the SCN mining window that is inclusive.
     *
     * The built query relies on the following columns from V$LOGMNR_CONTENTS:
     * <pre>
     *     SCN - the system change number at which the change was made
     *     SQL_REDO - the reconstructed SQL statement that initiated the change
     *     OPERATION - the database operation type name
     *     OPERATION_CODE - the database operation numeric code
     *     TIMESTAMP - the time when the change event occurred
     *     XID - the transaction identifier the change participated in
     *     CSF - the continuation flag, identifies rows that should be processed together as single row, 0=no, 1=yes
     *     TABLE_NAME - the name of the table for which the change is for
     *     SEG_OWNER - the name of the schema for which the change is for
     *     USERNAME - the name of the database user that caused the change
     *     ROW_ID - the unique identifier of the row that the change is for, may not always be set with valid value
     *     ROLLBACK - the rollback flag, value of 0 or 1.  1 implies the row was rolled back
     *     RS_ID - the rollback segment idenifier where the change record was record from
     * </pre>
     *
     * @param connectorConfig connector configuration, should not be {@code null}
     * @param schema database schema, should not be {@code null}
     * @return the SQL string to be used to fetch changes from Oracle LogMiner
     */
    public static String build(OracleConnectorConfig connectorConfig, OracleDatabaseSchema schema) {
        final StringBuilder query = new StringBuilder(1024);
        query.append(SELECT_LIST);

        // These bind parameters will be bound when the query is executed by the caller.
        query.append("WHERE SCN > ? AND SCN <= ? ");
        query.append(buildExcludedTransactionPredicate(connectorConfig, "XID"));

        // The connector currently requires a "database.pdb.name" configuration property when using CDB mode.
        // If this property is provided, build a predicate that will be used in later predicates.
        final String pdbName = connectorConfig.getPdbName();
        final String pdbPredicate;
        if (!Strings.isNullOrEmpty(pdbName)) {
            // This predicate is used later to explicitly restrict certain OPERATION_CODE and DDL events by the
            // PDB database name while allowing all START, COMMIT, MISSING_SCN, and ROLLBACK operations
            // regardless of where they originate, i.e. the PDB or CDB$ROOT.
            pdbPredicate = "SRC_CON_NAME = '" + pdbName + "'";
        }
        else {
            // No PDB configuration provided, no PDB predicate is necessary.
            pdbPredicate = null;
        }

        // Excluded schemas, if defined
        // This prevents things such as picking DDL for changes to LogMiner tables in SYSTEM tablespace
        // or picking up DML changes inside the SYS and SYSTEM tablespaces.
        final String excludedSchemas = resolveExcludedSchemaPredicate("SEG_OWNER");
        if (excludedSchemas.length() > 0) {
            query.append("AND ").append(excludedSchemas).append(' ');
        }

        query.append("AND (");

        // Always include START, COMMIT, MISSING_SCN, and ROLLBACK operations
        query.append("(OPERATION_CODE IN (6,7,34,36)");

        if (!schema.storeOnlyCapturedTables()) {
            // In this mode, the connector will always be fed DDL operations for all tables even if they
            // are not part of the inclusion/exclusion lists. We will pass the PDB predicate here to then
            // restrict DDL operations to only the PDB database if not null.
            query.append(" OR ").append(buildDdlPredicate(pdbPredicate)).append(" ");
            // Insert, Update, Delete, SelectLob, LobWrite, LobTrim, and LobErase
            if (connectorConfig.isLobEnabled()) {
                query.append(") OR (OPERATION_CODE IN (1,2,3,9,10,11,29) ");
            }
            else {
                // Only capture UNSUPPORTED operations (255) when LOB is disabled to avoid
                // the logging handler writing duplicate entries due to re-mining strategy
                query.append(") OR (OPERATION_CODE IN (1,2,3,255) ");
            }
            if (pdbPredicate != null) {
                // Restrict Insert, Update, Delete, and optionally SelectLob, LobWrite, LobTrim, and LobErase by PDB
                query.append("AND ").append(pdbPredicate).append(' ');
            }
        }
        else {
            query.append(") OR (");
            if (pdbPredicate != null) {
                // We specify the PDB predicate here because it applies to the OPERATION_CODE predicates but
                // also the DDL predicate that is to follow later due to predicate groups, effectively
                // restricting all DML operations and DDL changes to the PDB only.
                query.append(pdbPredicate).append(" AND ");
            }
            // Insert, Update, Delete, SelectLob, LobWrite, LobTrim, and LobErase
            if (connectorConfig.isLobEnabled()) {
                query.append("(OPERATION_CODE IN (1,2,3,9,10,11,29) ");
            }
            else {
                // Only capture UNSUPPORTED operations (255) when LOB is disabled to avoid
                // the logging handler writing duplicate entries due to re-mining strategy
                query.append("(OPERATION_CODE IN (1,2,3,255) ");
            }
            // In this mode, the connector will filter DDL operations based on the table inclusion/exclusion lists
            // We pass "null" to the DDL predicate because we will have added the predicate earlier as a part of
            // the outer predicate group to also be applied to OPERATION_CODE
            query.append("OR ").append(buildDdlPredicate(null)).append(") ");
        }

        // Always ignore the flush table
        query.append("AND TABLE_NAME != '").append(LogWriterFlushStrategy.LOGMNR_FLUSH_TABLE).append("' ");

        String schemaPredicate = buildSchemaPredicate(connectorConfig);
        if (!Strings.isNullOrEmpty(schemaPredicate)) {
            query.append("AND ").append(schemaPredicate).append(" ");
        }

        String tablePredicate = buildTablePredicate(connectorConfig);
        if (!Strings.isNullOrEmpty(tablePredicate)) {
            query.append("AND ").append(tablePredicate).append(" ");
        }

        query.append("))");

        return query.toString();
    }

    public static String buildPlSqlOutputBlock(OracleConnectorConfig connectorConfig, OracleDatabaseSchema schema) {
        final String dmlOperationCodes = connectorConfig.isLobEnabled() ? "1,2,3,9,10,11,29" : "1,2,3,255";
        final String pdbPredicate = Strings.isNullOrEmpty(connectorConfig.getPdbName()) ? null : "SRC_CON_NAME = '" + connectorConfig.getPdbName() + "'";
        final String rowPredicate = buildPlSqlCapturedTablePredicate(connectorConfig, "r.", dmlOperationCodes, pdbPredicate);
        final String ddlPredicate = buildPlSqlDdlPredicate(connectorConfig, schema, "r.", pdbPredicate);
        final String unorderedQuery = PLSQL_OUTPUT_SELECT_LIST.replace("SELECT ", "SELECT ROWNUM AS LOGMNR_ROW_SEQUENCE, ")
                .replace("FROM " + LOGMNR_CONTENTS_VIEW + " ", "FROM " + LOGMNR_CONTENTS_VIEW + " r ") +
                "WHERE r.SCN > l_window_start_scn AND r.SCN <= l_window_end_scn " +
                buildExcludedTransactionPredicate(connectorConfig, "r.XID") +
                "AND ((" + rowPredicate + ") " +
                "OR (r.OPERATION_CODE IN (7,34,36)) " +
                "OR (" + ddlPredicate + "))";
        final String query = "SELECT q.* FROM (" + unorderedQuery + ") q ORDER BY q.SCN, q.LOGMNR_ROW_SEQUENCE";

        return "DECLARE " +
                "l_window_start_scn NUMBER := ?; " +
                "l_window_end_scn NUMBER := ?; " +
                "l_output_bytes NUMBER := 0; " +
                "l_completed_scn_groups NUMBER := 0; " +
                "l_in_csf_group BOOLEAN := FALSE; " +
                "l_truncated BOOLEAN := FALSE; " +
                "l_header VARCHAR2(32767); " +
                "l_last_completed_scn VARCHAR2(64); " +
                // Reserved byte budget for the worst-case trailing '@END|<scn>|truncated' line.
                "l_end_reserve CONSTANT NUMBER := 64; " +
                "l_group_scn NUMBER; " +
                "l_have_group BOOLEAN := FALSE; " +
                "l_group_bytes NUMBER := 0; " +
                "l_group_line_count PLS_INTEGER := 0; " +
                "TYPE t_output_lines IS TABLE OF VARCHAR2(32767) INDEX BY PLS_INTEGER; " +
                "l_group_lines t_output_lines; " +
                "FUNCTION line_bytes(p_line IN VARCHAR2) RETURN NUMBER IS " +
                "BEGIN " +
                "RETURN NVL(LENGTHB(p_line), 0) + 1; " +
                "END; " +
                "PROCEDURE put_line(p_line IN VARCHAR2) IS " +
                "BEGIN " +
                "DBMS_OUTPUT.PUT_LINE(p_line); " +
                "l_output_bytes := l_output_bytes + line_bytes(p_line); " +
                "END; " +
                "PROCEDURE put_truncated_end IS " +
                "BEGIN " +
                "put_line('@END|' || NVL(l_last_completed_scn, '') || '|truncated'); " +
                "l_truncated := TRUE; " +
                "END; " +
                "PROCEDURE reset_group IS " +
                "BEGIN " +
                "l_group_lines.DELETE; " +
                "l_group_line_count := 0; " +
                "l_group_bytes := 0; " +
                "l_group_scn := NULL; " +
                "l_have_group := FALSE; " +
                "END; " +
                "PROCEDURE append_group_line(p_line IN VARCHAR2) IS " +
                "BEGIN " +
                "l_group_line_count := l_group_line_count + 1; " +
                "l_group_lines(l_group_line_count) := p_line; " +
                "l_group_bytes := l_group_bytes + line_bytes(p_line); " +
                "END; " +
                "PROCEDURE flush_group IS " +
                "BEGIN " +
                "FOR i IN 1..l_group_line_count LOOP " +
                "put_line(l_group_lines(i)); " +
                "END LOOP; " +
                "END; " +
                "PROCEDURE complete_group IS " +
                "BEGIN " +
                "IF NOT l_have_group THEN " +
                "RETURN; " +
                "END IF; " +
                "IF l_in_csf_group THEN " +
                "RAISE_APPLICATION_ERROR(-20002, 'PL/SQL output LogMiner SCN group ended with incomplete CSF row at SCN ' || TO_CHAR(l_group_scn)); " +
                "END IF; " +
                "flush_group; " +
                "l_completed_scn_groups := l_completed_scn_groups + 1; " +
                "l_last_completed_scn := TO_CHAR(l_group_scn); " +
                "reset_group; " +
                "END; " +
                "BEGIN " +
                "DBMS_OUTPUT.DISABLE; " +
                "DBMS_OUTPUT.ENABLE(NULL); " +
                "FOR r IN (" + query + ") LOOP " +
                "IF l_have_group AND r.SCN < l_group_scn THEN " +
                "RAISE_APPLICATION_ERROR(-20003, 'PL/SQL output LogMiner SCN out of order: ' || TO_CHAR(r.SCN) || ' after ' || TO_CHAR(l_group_scn)); " +
                "END IF; " +
                "IF l_have_group AND r.SCN > l_group_scn THEN " +
                "complete_group; " +
                "END IF; " +
                "IF NOT l_have_group THEN " +
                "l_group_scn := r.SCN; " +
                "l_have_group := TRUE; " +
                "END IF; " +
                "l_header := '@ROW|' || TO_CHAR(r.SCN) || '|' || TO_CHAR(r.OPERATION_CODE) || '|' || " +
                "TO_CHAR(r.CHANGE_TIME, 'YYYY-MM-DD HH24:MI:SS') || '|' || NVL(r.XID_HEX, '') || '|' || " +
                "TO_CHAR(NVL(r.CSF, 0)) || '|' || NVL(r.TABLE_NAME, '') || '|' || NVL(r.SEG_OWNER, '') || '|' || " +
                "NVL(r.OPERATION, '') || '|' || NVL(r.USERNAME, '') || '|' || NVL(r.ROW_ID, '') || '|' || " +
                "TO_CHAR(NVL(r.ROLLBACK_FLAG, 0)) || '|' || NVL(r.RS_ID, '') || '|' || TO_CHAR(NVL(r.STATUS, 0)) || '|' || " +
                "TO_CHAR(NVL(r.SSN, 0)) || '|' || TO_CHAR(NVL(r.THREAD_NUMBER, 0)) || '|' || " +
                "NVL(REPLACE(REPLACE(r.INFO, CHR(13), ' '), CHR(10), ' '), ''); " +
                "append_group_line(l_header); " +
                "append_group_line(r.SQL_REDO); " +
                "l_in_csf_group := NVL(r.CSF, 0) <> 0; " +
                "IF l_completed_scn_groups = 0 AND l_group_bytes + l_end_reserve > " + PLSQL_OUTPUT_MAX_BYTES + " THEN " +
                "RAISE_APPLICATION_ERROR(-20001, 'PL/SQL output LogMiner single SCN group exceeds DBMS_OUTPUT limit of " + PLSQL_OUTPUT_MAX_BYTES
                + " bytes at SCN ' || TO_CHAR(l_group_scn)); " +
                "END IF; " +
                "IF l_completed_scn_groups > 0 AND l_output_bytes + l_group_bytes + l_end_reserve > " + PLSQL_OUTPUT_MAX_BYTES + " THEN " +
                "put_truncated_end; " +
                "EXIT; " +
                "END IF; " +
                "END LOOP; " +
                "IF NOT l_truncated THEN " +
                "complete_group; " +
                "put_line('@END|' || TO_CHAR(l_window_end_scn) || '|complete'); " +
                "END IF; " +
                "END;";
    }

    private static String buildExcludedTransactionPredicate(OracleConnectorConfig connectorConfig, String xidColumn) {
        final Set<String> transactionIds = connectorConfig.getLogMiningTransactionExcludeIds();
        if (transactionIds.isEmpty()) {
            return "";
        }
        final String excludedIds = transactionIds.stream()
                .sorted()
                .map(transactionId -> "HEXTORAW('" + transactionId.toUpperCase() + "')")
                .collect(Collectors.joining(","));
        return "AND (" + xidColumn + " IS NULL OR " + xidColumn + " NOT IN (" + excludedIds + ")) ";
    }

    private static String buildPlSqlCapturedTablePredicate(OracleConnectorConfig connectorConfig, String alias, String dmlOperationCodes, String pdbPredicate) {
        final StringBuilder predicate = new StringBuilder(512);
        predicate.append(alias).append("OPERATION_CODE IN (").append(dmlOperationCodes).append(") ");
        if (pdbPredicate != null) {
            predicate.append("AND ").append(alias).append(pdbPredicate).append(' ');
        }

        final String excludedSchemas = resolveExcludedSchemaPredicate(alias + "SEG_OWNER");
        if (excludedSchemas.length() > 0) {
            predicate.append("AND ").append(excludedSchemas).append(' ');
        }

        predicate.append("AND NVL(").append(alias).append("TABLE_NAME, ").append(alias).append("SEG_NAME) != '")
                .append(LogWriterFlushStrategy.LOGMNR_FLUSH_TABLE).append("' ");

        final String schemaPredicate = buildSchemaPredicate(connectorConfig, alias + "SEG_OWNER");
        if (!Strings.isNullOrEmpty(schemaPredicate)) {
            predicate.append("AND ").append(schemaPredicate).append(' ');
        }

        final String tablePredicate = buildTablePredicate(connectorConfig, alias + "SEG_OWNER || '.' || NVL(" + alias + "TABLE_NAME, " + alias + "SEG_NAME)");
        if (!Strings.isNullOrEmpty(tablePredicate)) {
            predicate.append("AND ").append(tablePredicate).append(' ');
        }
        return predicate.toString();
    }

    private static String buildPlSqlDdlPredicate(OracleConnectorConfig connectorConfig, OracleDatabaseSchema schema, String alias, String pdbPredicate) {
        final StringBuilder predicate = new StringBuilder(512);
        predicate.append(alias).append("OPERATION_CODE = 5 ");
        predicate.append("AND ").append(alias).append("USERNAME NOT IN ('SYS','SYSTEM') ");
        predicate.append("AND ").append(alias).append("INFO NOT LIKE 'INTERNAL DDL%' ");
        predicate.append("AND (NVL(").append(alias).append("TABLE_NAME, ").append(alias).append("SEG_NAME) IS NULL OR NVL(")
                .append(alias).append("TABLE_NAME, ").append(alias).append("SEG_NAME) NOT LIKE 'ORA_TEMP_%') ");
        if (pdbPredicate != null) {
            predicate.append("AND ").append(alias).append(pdbPredicate).append(' ');
        }

        final String excludedSchemas = resolveExcludedSchemaPredicate(alias + "SEG_OWNER");
        if (excludedSchemas.length() > 0) {
            predicate.append("AND ").append(excludedSchemas).append(' ');
        }

        if (schema.storeOnlyCapturedTables()) {
            final String schemaPredicate = buildSchemaPredicate(connectorConfig, alias + "SEG_OWNER");
            if (!Strings.isNullOrEmpty(schemaPredicate)) {
                predicate.append("AND ").append(schemaPredicate).append(' ');
            }

            final String tablePredicate = buildTablePredicate(connectorConfig, alias + "SEG_OWNER || '.' || NVL(" + alias + "TABLE_NAME, " + alias + "SEG_NAME)");
            if (!Strings.isNullOrEmpty(tablePredicate)) {
                predicate.append("AND ").append(tablePredicate).append(' ');
            }
        }
        return predicate.toString();
    }

    /**
     * Builds a common SQL fragment used to obtain DDL operations via LogMiner.
     *
     * @param pdbPredicate pluggable database predicate, maybe {@code null}
     * @return predicate that can be used to obtain DDL operations via LogMiner
     */
    private static String buildDdlPredicate(String pdbPredicate) {
        final StringBuilder predicate = new StringBuilder(256);
        predicate.append("(OPERATION_CODE = 5 ");
        predicate.append("AND USERNAME NOT IN ('SYS','SYSTEM') ");
        predicate.append("AND INFO NOT LIKE 'INTERNAL DDL%' ");
        if (pdbPredicate != null) {
            // DDL changes should be restricted to only the PDB database if supplied
            predicate.append("AND ").append(pdbPredicate).append(' ');
        }
        predicate.append("AND (TABLE_NAME IS NULL OR TABLE_NAME NOT LIKE 'ORA_TEMP_%'))");
        return predicate.toString();
    }

    /**
     * Builds a SQL predicate of what schemas to include/exclude based on the connector configuration.
     *
     * @param connectorConfig connector configuration, should not be {@code null}
     * @return SQL predicate to filter results based on schema include/exclude configurations
     */
    private static String buildSchemaPredicate(OracleConnectorConfig connectorConfig) {
        return buildSchemaPredicate(connectorConfig, "SEG_OWNER");
    }

    private static String buildSchemaPredicate(OracleConnectorConfig connectorConfig, String columnName) {
        StringBuilder predicate = new StringBuilder();
        if (Strings.isNullOrEmpty(connectorConfig.schemaIncludeList())) {
            if (!Strings.isNullOrEmpty(connectorConfig.schemaExcludeList())) {
                List<Pattern> patterns = Strings.listOfRegex(connectorConfig.schemaExcludeList(), 0);
                predicate.append("(").append(listOfPatternsToSql(patterns, columnName, true)).append(")");
            }
        }
        else {
            List<Pattern> patterns = Strings.listOfRegex(connectorConfig.schemaIncludeList(), 0);
            predicate.append("(").append(listOfPatternsToSql(patterns, columnName, false)).append(")");
        }
        return predicate.toString();
    }

    /**
     * Builds a SQL predicate of what tables to include/exclude based on the connector configuration.
     *
     * @param connectorConfig connector configuration, should not be {@code null}
     * @return SQL predicate to filter results based on table include/exclude configuration
     */
    private static String buildTablePredicate(OracleConnectorConfig connectorConfig) {
        return buildTablePredicate(connectorConfig, "SEG_OWNER || '.' || TABLE_NAME");
    }

    private static String buildTablePredicate(OracleConnectorConfig connectorConfig, String columnName) {
        StringBuilder predicate = new StringBuilder();
        if (Strings.isNullOrEmpty(connectorConfig.tableIncludeList())) {
            if (!Strings.isNullOrEmpty(connectorConfig.tableExcludeList())) {
                List<Pattern> patterns = Strings.listOfRegex(connectorConfig.tableExcludeList(), 0);
                predicate.append("(").append(listOfPatternsToSql(patterns, columnName, true)).append(")");
            }
        }
        else {
            List<Pattern> patterns = Strings.listOfRegex(connectorConfig.tableIncludeList(), 0);
            predicate.append("(").append(listOfPatternsToSql(patterns, columnName, false)).append(")");
        }
        return predicate.toString();
    }

    /**
     * Takes a list of reg-ex patterns and builds an Oracle-specific predicate using {@code REGEXP_LIKE}
     * in order to take the connector configuration include/exclude lists and assemble them as SQL
     * predicates.
     *
     * @param patterns list of each individual include/exclude reg-ex patterns from connector configuration
     * @param columnName the column in which the reg-ex patterns are to be applied against
     * @param inclusion should be {@code true} when passing inclusion patterns, {@code false} otherwise
     * @return
     */
    private static String listOfPatternsToSql(List<Pattern> patterns, String columnName, boolean inclusion) {
        StringBuilder predicate = new StringBuilder();
        for (Iterator<Pattern> i = patterns.iterator(); i.hasNext();) {
            Pattern pattern = i.next();
            if (inclusion) {
                predicate.append("NOT ");
            }
            // NOTE: The REGEXP_LIKE operator was added in Oracle 10g (10.1.0.0.0)
            final String text = resolveRegExpLikePattern(pattern);
            predicate.append("REGEXP_LIKE(").append(columnName).append(",'").append(text).append("','i')");
            if (i.hasNext()) {
                // Exclude lists imply combining them via AND, Include lists imply combining them via OR?
                predicate.append(inclusion ? " AND " : " OR ");
            }
        }
        return predicate.toString();
    }

    /**
     * The {@code REGEXP_LIKE} Oracle operator acts identical to the {@code LIKE} operator. Internally,
     * it prepends and appends a "%" qualifier.  The include/exclude lists are meant to be explicit in
     * that they have an implied "^" and "$" qualifier for start/end so that the LIKE operation does
     * not mistakently filter "DEBEZIUM2" when using the reg-ex of "DEBEZIUM".
     *
     * @param pattern the pattern to be analyzed, should not be {@code null}
     * @return the adjusted predicate, if necessary and doesn't already explicitly specify "^" or "$"
     */
    private static String resolveRegExpLikePattern(Pattern pattern) {
        String text = pattern.pattern();
        if (!text.startsWith("^")) {
            text = "^" + text;
        }
        if (!text.endsWith("$")) {
            text += "$";
        }
        return text;
    }

    /**
     * Resolve the built-in excluded schemas predicate.
     *
     * @param fieldName the query field name the predicate applies to, should never be {@code null}
     * @return the predicate
     */
    private static String resolveExcludedSchemaPredicate(String fieldName) {
        // There are some common schemas that we automatically ignore when building the runtime Filter
        // predicates, and we put that same list of schemas here and apply those in the generated SQL.
        if (!OracleConnectorConfig.EXCLUDED_SCHEMAS.isEmpty()) {
            StringBuilder query = new StringBuilder();
            query.append('(').append(fieldName).append(" IS NULL OR ");
            query.append(fieldName).append(" NOT IN (");
            for (Iterator<String> i = OracleConnectorConfig.EXCLUDED_SCHEMAS.iterator(); i.hasNext();) {
                String excludedSchema = i.next();
                query.append('\'').append(excludedSchema.toUpperCase()).append('\'');
                if (i.hasNext()) {
                    query.append(',');
                }
            }
            return query.append(')').append(')').toString();
        }
        return "";
    }
}
