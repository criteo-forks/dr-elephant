package com.linkedin.drelephant.purge;

import com.avaje.ebean.Ebean;
import com.avaje.ebean.SqlRow;
import com.avaje.ebean.Transaction;
import java.util.List;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//Not thread safe because of the string builder. This class is intended to be called by one single thread.
public class AppResultPurger {

    private static final Logger logger = LoggerFactory.getLogger(AppResultPurger.class);
    public static final int LOGGING_THRESHOLD = 10000;

    private static StringBuilder sBuilder = new StringBuilder();

    public static int deleteOlderThan(int days, int batchSize) {

        logger.info("AppResults purge started...");

        int count = 0;
        int loopCount = 1;

        long resultsDeadline = DateTime.now().minusDays(days).getMillis();

        String findAppResultsQuery =
            "SELECT id FROM yarn_app_result WHERE finish_time < " + resultsDeadline + " LIMIT " + batchSize + ";";

        while (true) {

            logger.debug("Loop {} - trying to purge {} app results", loopCount, batchSize);

            Transaction transaction = null;

            try {
                String idsCommaSeparated = getDeletableAppResultsIds(findAppResultsQuery);

                if ("".equals(idsCommaSeparated)) {
                    logger.debug("Found nothing more to purge stop looping");
                    break;
                }

                transaction = Ebean.beginTransaction();

                String purgeHeuristicDetails = "DELETE yarn_app_heuristic_result_details " +
                    "FROM yarn_app_heuristic_result_details " +
                    "JOIN yarn_app_heuristic_result hres ON " +
                    "yarn_app_heuristic_result_id = hres.id " +
                    "WHERE hres.yarn_app_result_id in (" + idsCommaSeparated + ");";

                String purgeHeuristic = "DELETE " +
                    "FROM yarn_app_heuristic_result " +
                    "WHERE yarn_app_result_id in (" + idsCommaSeparated + ");";

                String purgeResults = "DELETE FROM yarn_app_result WHERE id in (" + idsCommaSeparated + ");";

                Ebean.createSqlUpdate(purgeHeuristicDetails).execute();
                Ebean.createSqlUpdate(purgeHeuristic).execute();
                count += Ebean.createSqlUpdate(purgeResults).execute();

                transaction.commit();

                if (count % LOGGING_THRESHOLD == 0) {
                    logger.info("Purge still in progress, already purged {} app results", count);
                }

            } catch (Exception e) {
                logger.error("Unexpected error occurred while purging old app results", e);
                throw new RuntimeException(e);
            } finally {
                if (transaction != null) {
                    transaction.end();
                }
            }

            logger.debug("Loop {} - loop execution finished. Total app results deleted is {}", loopCount, count);

            loopCount++;
        }

        logger.info("AppResults purge finished. Purged " + count + " records older than " + days + " day(s) from the database");

        return count;

    }

    private static String getDeletableAppResultsIds(String findAppResultsQuery) {
        List<SqlRow> rows = Ebean.createSqlQuery(findAppResultsQuery).findList();
        sBuilder.setLength(0); //fast reset of string builder
        for (SqlRow row : rows) {
            sBuilder.append("'").append(row.getString("id")).append("'").append(",");
        }
        if (sBuilder.length() > 0) {
            sBuilder.setLength(sBuilder.length() - 1); //remove trailing , if needed
        }
        return sBuilder.toString();
    }

}
