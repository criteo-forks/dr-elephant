package com.criteo.drelephant.heuristics;

import com.avaje.ebean.Ebean;
import com.avaje.ebean.SqlRow;
import com.avaje.ebean.Transaction;
import com.linkedin.drelephant.analysis.Severity;
import com.linkedin.drelephant.util.Utils;
import models.AppHeuristicResult;
import models.AppHeuristicResultDetails;
import models.AppResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class GarmadonTransferHeuristic {
    private static final Logger LOGGER = LoggerFactory.getLogger(GarmadonTransferHeuristic.class);

    private static final Integer MAX_READ_TIMES = 15;

    private static final String TABLENAME_PREFIX = "garmadon_";
    private static final String HEURISTIC_RESULT_TABLENAME = TABLENAME_PREFIX + "yarn_app_heuristic_result";
    private static final String HEURISTIC_RESULT_DETAILS_TABLENAME = TABLENAME_PREFIX + "yarn_app_heuristic_result_details";

    private static final String SELECT_ALL_DISTINCT_APP_SQL = "SELECT DISTINCT yarn_app_result_id FROM " + HEURISTIC_RESULT_TABLENAME
            + " WHERE ready = 1";
    private static final String SELECT_HEURISTIC_RESULT_SQL = "SELECT * FROM " + HEURISTIC_RESULT_TABLENAME
            + " WHERE yarn_app_result_id = :yarn_app_result_id";
    private static final String DELETE_HEURISTIC_RESULT_SQL = "DELETE FROM " + HEURISTIC_RESULT_TABLENAME
            + " WHERE yarn_app_result_id = :yarn_app_result_id";
    private static final String SELECT_HEURISTIC_RESULT_DETAILS_SQL = "SELECT * FROM " + HEURISTIC_RESULT_DETAILS_TABLENAME
            + " WHERE yarn_app_heuristic_result_id = :yarn_app_heuristic_result_id";
    private static final String DELETE_HEURISTIC_RESULT_DETAILS_SQL = "DELETE t1 FROM " + HEURISTIC_RESULT_DETAILS_TABLENAME + " t1"
            + " INNER JOIN " + HEURISTIC_RESULT_TABLENAME + " t2"
            + " ON t1.yarn_app_heuristic_result_id = t2.id"
            + " WHERE t2.yarn_app_result_id = :yarn_app_result_id";
    private static final String UPDATE_READ_TIMES_APP_RESULT_SQL = "UPDATE " + HEURISTIC_RESULT_TABLENAME
            + " SET read_times = read_times + 1"
            + " WHERE yarn_app_result_id = :yarn_app_result_id";
    private static final String GET_MAX_READ_TIMES_APP_RESULT_SQL = "SELECT MAX(read_times) AS max_read_times FROM "
            + HEURISTIC_RESULT_TABLENAME
            + " WHERE yarn_app_result_id = :yarn_app_result_id";

    private static final int MAX_ROW = 10000;

    public static void transfer() {
        int nbRow = 0;

        // Select all app in garmadon table
        try {
            LOGGER.debug("Select all distinct ready yarn_app_result_id from {}", HEURISTIC_RESULT_TABLENAME);
            List<SqlRow> rows = Ebean.createSqlQuery(SELECT_ALL_DISTINCT_APP_SQL).findList();

            Ebean.beginTransaction();
            try {
                for (SqlRow row : rows) {
                    nbRow++;
                    String yarn_app_result_id = row.getString("yarn_app_result_id");
                    AppResult appResult = Ebean.find(AppResult.class, yarn_app_result_id);
                    if (appResult != null) {
                        LOGGER.info("Insert data for {}", yarn_app_result_id);
                        insertData(appResult, yarn_app_result_id);
                    } else {
                        treatResultNotInDrElephantDb(yarn_app_result_id);
                    }
                    if (nbRow >= MAX_ROW) {
                        nbRow = 0;
                        Ebean.commitTransaction();
                        Ebean.beginTransaction();
                    }
                }
                Ebean.commitTransaction();
            } catch (Exception e) {
                LOGGER.error("Unexpected error occurred while deleting useless garmadon heuristics", e);
                throw new RuntimeException(e);
            } finally {
                Ebean.endTransaction();
            }
        } catch (Exception e) {
            LOGGER.error("Unexpected error occurred while reading garmadon heuristics", e);
        }
    }

    private static void treatResultNotInDrElephantDb(String yarn_app_result_id) {
        SqlRow rowMaxRead = Ebean.createSqlQuery(GET_MAX_READ_TIMES_APP_RESULT_SQL)
                .setParameter("yarn_app_result_id", yarn_app_result_id)
                .findUnique();

        if (rowMaxRead.getInteger("max_read_times") > MAX_READ_TIMES) {

            // Delete this app result
            LOGGER.info("Delete data from {} because still not in dr-elephant after {} times",
                    yarn_app_result_id, MAX_READ_TIMES);
            deleteData(yarn_app_result_id);
        } else {
            // Increment number of time we read this app result
            Ebean.createSqlUpdate(UPDATE_READ_TIMES_APP_RESULT_SQL)
                    .setParameter("yarn_app_result_id", yarn_app_result_id)
                    .execute();
        }
    }

    private static void insertData(AppResult appResult, String yarn_app_result_id) {
        try {
            getHeuristics(yarn_app_result_id, appResult);

            LOGGER.debug("Save appResult {}", appResult.id);
            appResult.save();
        } catch (Exception e) {
            LOGGER.error("Unexpected error occurred while inserting garmadon heuristics", e);
            throw new RuntimeException(e);
        }
    }

    private static void getHeuristics(String yarn_app_result_id, AppResult appResult) {
        // Set severity
        Severity worstSeverity = appResult.severity;

        List<SqlRow> rows = Ebean.createSqlQuery(SELECT_HEURISTIC_RESULT_SQL)
                .setParameter("yarn_app_result_id", yarn_app_result_id)
                .findList();
        for (SqlRow row : rows) {
            AppHeuristicResult appHeuristicResult = new AppHeuristicResult();
            String id = row.getString("id");
            appHeuristicResult.heuristicClass = Utils.truncateField(row.getString("heuristic_class"),
                    AppHeuristicResult.HEURISTIC_CLASS_LIMIT, id);
            appHeuristicResult.heuristicName = Utils.truncateField(row.getString("heuristic_name"),
                    AppHeuristicResult.HEURISTIC_NAME_LIMIT, id);
            appHeuristicResult.score = row.getInteger("score");
            appHeuristicResult.severity = Severity.byValue(row.getInteger("severity"));

            getHeuristicsDetail(id, appHeuristicResult);

            appResult.yarnAppHeuristicResults.add(appHeuristicResult);
            worstSeverity = Severity.max(worstSeverity, appHeuristicResult.severity);
        }
        appResult.severity = worstSeverity;
        deleteData(yarn_app_result_id);
    }

    private static void deleteData(String yarn_app_result_id) {
        LOGGER.debug("Delete yarn_app_result_id {} on {}", yarn_app_result_id, HEURISTIC_RESULT_DETAILS_TABLENAME);
        Ebean.createSqlUpdate(DELETE_HEURISTIC_RESULT_DETAILS_SQL)
                .setParameter("yarn_app_result_id", yarn_app_result_id)
                .execute();
        LOGGER.debug("Delete yarn_app_result_id {} on {}", yarn_app_result_id, HEURISTIC_RESULT_TABLENAME);
        Ebean.createSqlUpdate(DELETE_HEURISTIC_RESULT_SQL)
                .setParameter("yarn_app_result_id", yarn_app_result_id)
                .execute();
    }

    private static void getHeuristicsDetail(String id, AppHeuristicResult appHeuristicResult) {
        List<SqlRow> detail_rows = Ebean.createSqlQuery(SELECT_HEURISTIC_RESULT_DETAILS_SQL)
                .setParameter("yarn_app_heuristic_result_id", id)
                .findList();

        for (SqlRow detail_row : detail_rows) {

            AppHeuristicResultDetails heuristicDetail = new AppHeuristicResultDetails();
            heuristicDetail.name = Utils.truncateField(detail_row.getString("name"),
                    AppHeuristicResultDetails.NAME_LIMIT, id);
            heuristicDetail.value = Utils.truncateField(detail_row.getString("value"),
                    AppHeuristicResultDetails.VALUE_LIMIT, id);
            heuristicDetail.details = Utils.truncateField(detail_row.getString("details"),
                    AppHeuristicResultDetails.DETAILS_LIMIT, id);
            heuristicDetail.yarnAppHeuristicResult = appHeuristicResult;
            appHeuristicResult.yarnAppHeuristicResultDetails.add(heuristicDetail);
        }
    }

}
