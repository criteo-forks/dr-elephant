package com.linkedin.drelephant.garmadon;

import com.linkedin.drelephant.analysis.HadoopApplicationData;
import com.linkedin.drelephant.analysis.Heuristic;
import com.linkedin.drelephant.analysis.HeuristicResult;
import com.linkedin.drelephant.analysis.Severity;
import com.linkedin.drelephant.configurations.heuristic.HeuristicConfigurationData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class TransferHeuristic implements Heuristic<HadoopApplicationData> {
    private static final Logger LOGGER = LoggerFactory.getLogger(TransferHeuristic.class);
    private static final String HEURISTIC_RESULT_TABLENAME = "yarn_app_heuristic_result";
    private static final String HEURISTIC_RESULT_DETAILS_TABLENAME = "yarn_app_heuristic_result_details";
    private static final String SELECT_HEURISTIC_RESULT_SQL = "SELECT * FROM " + HEURISTIC_RESULT_TABLENAME
            + "WHERE yarn_app_result_id = ?";
    private static final String SELECT_HEURISTIC_RESULT_DETAILS_SQL = "SELECT * FROM " + HEURISTIC_RESULT_DETAILS_TABLENAME
            + "WHERE yarn_app_heuristic_result_id = ?";

    private final HeuristicConfigurationData heuristicConfData;
    private final Connection connection;
    private final PreparedStatement selectHeuristicResultStat;
    private final PreparedStatement selectHeuristicResultDetailsStat;

    public TransferHeuristic(HeuristicConfigurationData heuristicConfData) {
        this.heuristicConfData = heuristicConfData;
        String garmadonConnectionString = heuristicConfData.getParamMap().get("connectionString");
        String userName = heuristicConfData.getParamMap().get("username");
        String password = heuristicConfData.getParamMap().get("password");
        if (garmadonConnectionString == null)
            throw new IllegalArgumentException("Null JDBC connection string");
        if (userName == null)
            throw new IllegalArgumentException("Null JDBC username");
        try {
            connection = DriverManager.getConnection(garmadonConnectionString, userName, password);
            LOGGER.info("Connected successfully to {}", garmadonConnectionString);
        } catch (SQLException ex) {
            LOGGER.error("Cannot get JDBC connection with: {}", garmadonConnectionString, ex);
            throw new RuntimeException(ex);
        }
        selectHeuristicResultStat = prepareStatements(SELECT_HEURISTIC_RESULT_SQL);
        selectHeuristicResultDetailsStat = prepareStatements(SELECT_HEURISTIC_RESULT_DETAILS_SQL);
    }

    @Override
    public HeuristicResult apply(HadoopApplicationData data) {
        try {
            selectHeuristicResultStat.clearParameters();
            selectHeuristicResultStat.setString(1, data.getAppId());
            ResultSet heuristicResultSet = selectHeuristicResultStat.executeQuery();
            try {
                while (heuristicResultSet.next()) {
                    String id = heuristicResultSet.getString(1);
                    String heuristic_class = heuristicResultSet.getString(3);
                    String heuristic_name = heuristicResultSet.getString(4);
                    int severity = heuristicResultSet.getInt(5);
                    int score = heuristicResultSet.getInt(6);
                    HeuristicResult heuristicResult = new HeuristicResult(heuristic_class, heuristic_name, Severity.byValue(severity), score);
                    LOGGER.info("Adding heuristic: {}", heuristic_name);
                    selectHeuristicResultDetailsStat.clearParameters();
                    selectHeuristicResultDetailsStat.setString(1, id);
                    ResultSet detailsResultSet = selectHeuristicResultDetailsStat.executeQuery();
                    try {
                        while (detailsResultSet.next()) {
                            String name = detailsResultSet.getString(2);
                            String value = detailsResultSet.getString(3);
                            String details = detailsResultSet.getString(4);
                            heuristicResult.addResultDetail(name, value, details);
                        }
                    } finally {
                        detailsResultSet.close();
                    }
                }
            } finally {
                heuristicResultSet.close();
            }
        } catch (SQLException ex) {
            LOGGER.warn("Error when selecting from {} table", HEURISTIC_RESULT_TABLENAME, ex);
            return null;
        }
        return null;
    }

    @Override
    public HeuristicConfigurationData getHeuristicConfData() {
        return heuristicConfData;
    }

    private PreparedStatement prepareStatements(String sql) {
        try {
            return connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
        } catch (SQLException ex) {
            LOGGER.error("Cannot prepare statement: {}", sql, ex);
            throw new RuntimeException(ex);
        }
    }
}
