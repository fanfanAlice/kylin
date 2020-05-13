package org.apache.kylin.query.pushdown;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.DBUtils;
import org.apache.kylin.metadata.querymeta.SelectedColumnMeta;
import org.apache.kylin.sdk.datasource.framework.JdbcConnector;
import org.apache.kylin.sdk.datasource.framework.SourceConnectorFactory;
import org.apache.kylin.source.adhocquery.AbstractPushdownRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.List;

public class PrestoPushDownRunnerJDBCImpl extends AbstractPushdownRunner {

    public static final Logger logger = LoggerFactory.getLogger(PrestoPushDownRunnerJDBCImpl.class);

    private JdbcConnector dataSource;

    @Override
    public void init(KylinConfig config) {
        dataSource = SourceConnectorFactory.getPushDownConnector(config);
    }

    @Override
    public void executeQuery(String sql, List<List<String>> results, List<SelectedColumnMeta> columnMetas) throws Exception {
        //extract column metadata
        ResultSet rs = null;
        ResultSetMetaData metaData;
        int columnCount;
        try (Connection conn = dataSource.getConnection(); Statement state = conn.createStatement()) {
            rs = state.executeQuery(sql);

            extractResults(rs, results);
            metaData = rs.getMetaData();
            columnCount = metaData.getColumnCount();

            // fill in selected column meta
            for (int i = 1; i <= columnCount; ++i) {
                int kylinTypeId = dataSource.toKylinTypeId(metaData.getColumnTypeName(i), metaData.getColumnType(i));
                String kylinTypeName = dataSource.toKylinTypeName(kylinTypeId);
                columnMetas.add(new SelectedColumnMeta(metaData.isAutoIncrement(i), metaData.isCaseSensitive(i), false,
                        metaData.isCurrency(i), metaData.isNullable(i), false, metaData.getColumnDisplaySize(i),
                        metaData.getColumnLabel(i), metaData.getColumnName(i), null, null, null,
                        metaData.getPrecision(i), metaData.getScale(i), kylinTypeId, kylinTypeName,
                        metaData.isReadOnly(i), false, false));
            }
        } catch (Exception e) {
            throw new RuntimeException("executeQuery failed", e);
        } finally {
            DBUtils.closeQuietly(rs);
        }
    }

    @Override
    public void executeUpdate(String sql) throws Exception {
        try {
            dataSource.executeUpdate(sql);
        } catch (Exception e) {
            throw new RuntimeException("executeUpdate failed", e);
        }
    }

    @Override
    public String convertSql(KylinConfig kylinConfig, String sql, String project, String defaultSchema,
                             boolean isPrepare) {
        String converted = sql;

        // SDK convert
        String ret = dataSource.convertSql(converted);
        if (!converted.equals(ret)) {
            logger.debug("the query is converted to {} after applying SDK converter.", ret);
        }
        return ret;
    }

    public void extractResults(ResultSet resultSet, List<List<String>> results) throws SQLException {
        List<String> oneRow = new LinkedList<>();

        while (resultSet.next()) {
            for (int i = 0; i < resultSet.getMetaData().getColumnCount(); i++) {
                oneRow.add((resultSet.getString(i + 1)));
            }

            results.add(new LinkedList<>(oneRow));
            oneRow.clear();
        }
    }
}
