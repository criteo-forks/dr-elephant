package com.linkedin.drelephant.purge;

import static common.DBTestUtil.initDB;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static play.test.Helpers.fakeApplication;
import static play.test.Helpers.inMemoryDatabase;

import com.avaje.ebean.Ebean;
import java.util.List;
import models.AppHeuristicResult;
import models.AppHeuristicResultDetails;
import models.AppResult;
import org.junit.Before;
import org.junit.Test;
import play.test.FakeApplication;
import play.test.WithApplication;

public class AppResultPurgerTest extends WithApplication {

    public static FakeApplication app;

    @Before
    public void setUp() {
        start(fakeApplication(inMemoryDatabase()));
        populateTestData();

        List<AppResult> appResults = Ebean.find(AppResult.class).findList();
        List<AppHeuristicResult> appHeuristicResults = Ebean.find(AppHeuristicResult.class).findList();
        List<AppHeuristicResultDetails> appHeuristicResultDetails = Ebean.find(AppHeuristicResultDetails.class).findList();

        assertTrue("app results present in database", appResults.size() > 0);
        assertTrue("app heuristic results present in database", appHeuristicResults.size() > 0);
        assertTrue("app heuristic results present in database", appHeuristicResultDetails.size() > 0);
    }

    //@Test deactivated because won't work with h2 database
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
