package com.criteo.drelephant.heuristics;

import com.avaje.ebean.Ebean;
import com.avaje.ebean.SqlRow;
import com.linkedin.drelephant.analysis.Severity;
import com.wix.mysql.EmbeddedMysql;
import com.wix.mysql.config.MysqldConfig;
import models.AppHeuristicResult;
import models.AppHeuristicResultDetails;
import models.AppResult;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import play.test.WithApplication;

import java.net.ServerSocket;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.wix.mysql.EmbeddedMysql.anEmbeddedMysql;
import static com.wix.mysql.ScriptResolver.classPathScript;
import static com.wix.mysql.config.Charset.UTF8;
import static com.wix.mysql.config.MysqldConfig.aMysqldConfig;
import static com.wix.mysql.distribution.Version.v5_7_latest;
import static common.TestConstants.*;
import static org.junit.Assert.*;
import static play.test.Helpers.fakeApplication;

public class GarmadonTransferHeuristicTest extends WithApplication {

    private static final String GET_MAX_READ_TIMES_APP_RESULT_SQL = "SELECT MAX(read_times) AS max_read_times FROM"
        + " garmadon_yarn_app_heuristic_result"
        + " WHERE yarn_app_result_id = :yarn_app_result_id";
    private static final String COUNT_APP_RESULT_SQL = "SELECT count(*) AS count_app FROM"
        + " garmadon_yarn_app_heuristic_result"
        + " WHERE yarn_app_result_id = :yarn_app_result_id";

    private EmbeddedMysql mysqld;

    private void initDB() {
        try {
            // Get one free port
            ServerSocket s = new ServerSocket(0);
            int port = s.getLocalPort();

            // Config Embedded Mysql
            MysqldConfig config = aMysqldConfig(v5_7_latest)
                .withCharset(UTF8)
                .withPort(port)
                .withUser("drelephant", "drelephant")
                .build();

            s.close();

            // Start Mysql DB
            mysqld = anEmbeddedMysql(config)
                .addSchema("drelephant")
                .start();

            Map<String, String> dbConn = new HashMap<String, String>();
            dbConn.put(DB_DEFAULT_DRIVER_KEY, "com.mysql.jdbc.Driver");
            dbConn.put(DB_DEFAULT_URL_KEY, "mysql://drelephant:drelephant@localhost:" + port + "/drelephant");
            dbConn.put(EVOLUTION_PLUGIN_KEY, EVOLUTION_PLUGIN_VALUE);
            dbConn.put(APPLY_EVOLUTIONS_DEFAULT_KEY, APPLY_EVOLUTIONS_DEFAULT_VALUE);

            start(fakeApplication(dbConn));

            mysqld.executeScripts("drelephant", classPathScript("test-init.sql"));

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Before
    public void setUp() {
        initDB();

        AppResult appResult = Ebean.find(AppResult.class, "application_1458194917883_1453361");
        assertEquals("Severity should be initialized at NONE", Severity.NONE, appResult.severity);

        AppHeuristicResult appHeuristicResults = Ebean.find(AppHeuristicResult.class)
            .where()
            .eq("yarn_app_result_id", appResult.id)
            .eq("heuristic_name", "moderate")
            .findUnique();

        assertNull("A moderate heuristic must not exist", appHeuristicResults);
    }

    @After
    public void setDown() {
        mysqld.stop();
    }

    @Test
    public void shouldPushGarmadonHeuristicsToApp() {
        GarmadonTransferHeuristic.transfer();
        AppResult appResult = Ebean.find(AppResult.class, "application_1458194917883_1453361");
        assertEquals("Severity should have been updated to CRITICAL", Severity.CRITICAL, appResult.severity);

        AppHeuristicResult appHeuristicResults = Ebean.find(AppHeuristicResult.class)
            .where()
            .eq("yarn_app_result_id", appResult.id)
            .eq("heuristic_name", "moderate")
            .findUnique();

        assertNotNull("A moderate heuristic must have been created", appHeuristicResults);

        List<AppHeuristicResultDetails> appHeuristicResultDetails = Ebean.find(AppHeuristicResultDetails.class)
            .where().eq("yarn_app_heuristic_result_id", appHeuristicResults.id)
            .findList();
        assertEquals("Some detail heuristics must have been added to the moderate heuristic", 3, appHeuristicResultDetails.size());
    }

    @Test
    public void shouldNotPushGarmadonHeuristicsToAppAsItIsNotReady() {
        GarmadonTransferHeuristic.transfer();
        AppResult appResult = Ebean.find(AppResult.class, "application_1458194917883_1453362");

        AppHeuristicResult appHeuristicResults = Ebean.find(AppHeuristicResult.class)
            .where()
            .eq("yarn_app_result_id", appResult.id)
            .eq("heuristic_name", "moderate")
            .findUnique();

        assertNull("A moderate heuristic must not exist", appHeuristicResults);
    }

    @Test
    public void shouldIncrementCounterReadTimesAndThenDeleteEntry() {
        SqlRow rowMaxRead = Ebean.createSqlQuery(GET_MAX_READ_TIMES_APP_RESULT_SQL)
            .setParameter("yarn_app_result_id", "application_1458194917883_1453363")
            .findUnique();
        assertTrue(13 == rowMaxRead.getInteger("max_read_times"));

        GarmadonTransferHeuristic.transfer();

        rowMaxRead = Ebean.createSqlQuery(GET_MAX_READ_TIMES_APP_RESULT_SQL)
            .setParameter("yarn_app_result_id", "application_1458194917883_1453363")
            .findUnique();
        assertTrue(14 == rowMaxRead.getInteger("max_read_times"));

        GarmadonTransferHeuristic.transfer();
        GarmadonTransferHeuristic.transfer();
        GarmadonTransferHeuristic.transfer();

        SqlRow countRow = Ebean.createSqlQuery(COUNT_APP_RESULT_SQL)
            .setParameter("yarn_app_result_id", "application_1458194917883_1453363")
            .findUnique();
        assertTrue(0 == countRow.getInteger("count_app"));
    }
}
