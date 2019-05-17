package com.linkedin.drelephant.purge;

import static com.wix.mysql.EmbeddedMysql.anEmbeddedMysql;
import static com.wix.mysql.ScriptResolver.classPathScript;
import static com.wix.mysql.config.Charset.UTF8;
import static com.wix.mysql.config.MysqldConfig.aMysqldConfig;
import static com.wix.mysql.distribution.Version.v5_7_latest;
import static common.DBTestUtil.initDB;
import static common.TestConstants.*;
import static common.TestConstants.APPLY_EVOLUTIONS_DEFAULT_VALUE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static play.test.Helpers.fakeApplication;
import static play.test.Helpers.inMemoryDatabase;

import com.avaje.ebean.Ebean;

import java.net.ServerSocket;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.wix.mysql.EmbeddedMysql;
import com.wix.mysql.config.MysqldConfig;
import models.AppHeuristicResult;
import models.AppHeuristicResultDetails;
import models.AppResult;
import org.junit.Before;
import org.junit.Test;
import play.Application;
import play.GlobalSettings;
import play.test.FakeApplication;
import play.test.WithApplication;

public class AppResultPurgerTest extends WithApplication {

    public static FakeApplication app;

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

        List<AppResult> appResults = Ebean.find(AppResult.class).findList();
        List<AppHeuristicResult> appHeuristicResults = Ebean.find(AppHeuristicResult.class).findList();
        List<AppHeuristicResultDetails> appHeuristicResultDetails = Ebean.find(AppHeuristicResultDetails.class).findList();

        assertTrue("app results present in database", appResults.size() > 0);
        assertTrue("app heuristic results present in database", appHeuristicResults.size() > 0);
        assertTrue("app heuristic results present in database", appHeuristicResultDetails.size() > 0);
    }

    @Test
    public void shouldDeleteOldAppResultsWithSiblings() {
        int count = AppResultPurger.deleteOlderThan(1, 1000);
        assertEquals("number of deletions counted", 2, count);

        List<AppResult> appResults = Ebean.find(AppResult.class).findList();
        List<AppHeuristicResult> appHeuristicResults = Ebean.find(AppHeuristicResult.class).findList();
        List<AppHeuristicResultDetails> appHeuristicResultDetails = Ebean.find(AppHeuristicResultDetails.class).findList();

        assertEquals("old app results are deleted", 0, appResults.size());
        assertEquals("related app heuristic results are deleted", 0, appHeuristicResults.size());
        assertEquals("related app heuristic results details are deleted", 0, appHeuristicResultDetails.size());
    }

    private void populateTestData() {
        try {
            initDB();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
