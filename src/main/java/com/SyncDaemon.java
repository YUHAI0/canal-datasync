package com;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.protocol.CanalEntry.Column;
import com.alibaba.otter.canal.protocol.CanalEntry.Entry;
import com.alibaba.otter.canal.protocol.CanalEntry.EntryType;
import com.alibaba.otter.canal.protocol.CanalEntry.EventType;
import com.alibaba.otter.canal.protocol.CanalEntry.RowChange;
import com.alibaba.otter.canal.protocol.CanalEntry.RowData;

@Slf4j
@Component
public class SyncDaemon implements Runnable {

    @Autowired
    private SqlHelper sqlHelper;

    @Autowired
    private CanalConfig canalConfig;

    @Autowired
    private CanalSinkConfig canalSinkConfig;

    @Override
    public void run() {
        // 创建链接
        String host = canalConfig.getHost();
        int port = canalConfig.getPort();
        String dest = canalConfig.getDest();
        String user = canalConfig.getUser();
        String password = canalConfig.getPassword();

        CanalConnector connector = CanalConnectors.newSingleConnector(
                new InetSocketAddress(host, port), dest, user, password);

        int batchSize = 1000;
        log.info("Canal Sync Daemon Started.");
        try {
            connector.connect();
            connector.subscribe(canalConfig.getFilterRegex());
            connector.rollback();
            while (true) {
                // 获取指定数量的数据
                Message message = connector.getWithoutAck(batchSize);
                long batchId = message.getId();
                int size = message.getEntries().size();
                if (batchId == -1 || size == 0) {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                    }
                } else {
                    log.info("message[batchId={}, size={}] \n", batchId, size);
                    handleEntries(message.getEntries());
                }
                // 提交确认
                connector.ack(batchId);
            }
        } finally {
            connector.disconnect();
        }
    }

    private void handleEntries(List<Entry> entries) {
        for (Entry entry : entries) {
            if (entry.getEntryType() == EntryType.TRANSACTIONBEGIN || entry.getEntryType() == EntryType.TRANSACTIONEND) {
                continue;
            }

            log.debug("Handle entry's type: {}", entry.getEntryType());

            RowChange rowChange = null;
            try {
                rowChange = RowChange.parseFrom(entry.getStoreValue());
                log.debug("SQL: {}", rowChange.getSql());
            } catch (Exception e) {
                throw new RuntimeException("ERROR ## parser of eromanga-event has an error , data:" + entry.toString(), e);
            }

            EventType eventType = rowChange.getEventType();
            log.debug(String.format("================&gt; binlog[%s:%s] , name[%s,%s] , eventType : %s",
                    entry.getHeader().getLogfileName(), entry.getHeader().getLogfileOffset(),
                    entry.getHeader().getSchemaName(), entry.getHeader().getTableName(),
                    eventType));

            try {
                doSync(entry.getHeader().getSchemaName(), entry);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private static void printColumn(List<Column> columns) {
        for (Column column : columns) {
            System.out.println(column.getName() + " : " + column.getValue() + "    update=" + column.getUpdated());
        }
    }

    private static Set<Integer> stringTypeValueSet = null;

    private static boolean needQuote(int sqlType) {
        if (stringTypeValueSet == null) {
            stringTypeValueSet = new HashSet<>();
            SupportSqlType.stringTypes().forEach(t -> {
                stringTypeValueSet.add(t.getValue());
            });
        }
        return stringTypeValueSet.contains(sqlType);
    }

    private void doSync(String dbName, Entry entry) {
        RowChange rowChange;
        try {
            rowChange = RowChange.parseFrom(entry.getStoreValue());
        } catch (Exception e) {
            throw new RuntimeException("ERROR ## parser of eromanga-event has an error , data:" + entry.toString(), e);
        }
        EventType eventType = rowChange.getEventType();

        // DDL操作
        log.debug("ignore DDL: {}", canalSinkConfig.isIgnoreDDL());
        // TODO: ddl有bug, ddl会包含dbname，所以必须要求源表与目标表的dbname相同
        if (!canalSinkConfig.isIgnoreDDL()) {
            if ( eventType == EventType.CREATE
                    || eventType == EventType.ALTER
                    || eventType == EventType.TRUNCATE
                    || eventType == EventType.RENAME
            ) {
                String sql = rowChange.getSql();
                log.debug("create sql: {}", sql);
                sqlHelper.doSql(dbName, sql);
                return;
            }
        }

        for (RowData rowData : rowChange.getRowDatasList()) {
            if (eventType == EventType.DELETE) {
                List<CanalEntry.Column> columns = rowData.getBeforeColumnsList();
                String sql = buildDeleteSql(entry.getHeader().getTableName(), columns);
                log.debug("Gen Delete sql: " + sql);
                sqlHelper.doSql(dbName, sql);
            } else if (eventType == EventType.INSERT) {
                List<CanalEntry.Column> columns = rowData.getAfterColumnsList();
                String sql = buildInsertSql(entry.getHeader().getTableName(), columns);
                log.debug("Gen Insert sql: " + sql);
                sqlHelper.doSql(dbName, sql);
            } else if (eventType == EventType.UPDATE) {
                List<Column> beforeColumns = rowData.getBeforeColumnsList();
                List<Column> afterColumns = rowData.getAfterColumnsList();
                String sql = buildUpdateSql(entry.getHeader().getTableName(), beforeColumns, afterColumns);
                log.debug("Gen Update sql: " + sql);
                sqlHelper.doSql(dbName, sql);
            } else {
                log.warn("Occur Other event, type: " + eventType);
            }
        }
    }

    private static String buildUpdateSql(String table, List<Column> beforeColumns, List<Column> afterColumns) {
        if (beforeColumns.size() == 0 || afterColumns.size() == 0) {
            throw new RuntimeException("before or after columns size is zero, can't build update sql");
        }

        StringBuilder sql = new StringBuilder("update ").append(table).append(" set ");

        for (Column ac : afterColumns) {
            if ("".equals(ac.getValue()) && ac.getIsNull()) {
                continue;
            }

            log.debug(String.format("%s, %s, sqlType: %s\n", ac.getName(), ac.getValue(), ac.getSqlType()));

            if (needQuote(ac.getSqlType())) {
                sql.append(ac.getName()).append("=").append("'").append(ac.getValue()).append("'");
            } else {
                sql.append(ac.getName()).append("=").append(ac.getValue());
            }
            sql.append(",");
        }
        sql.deleteCharAt(sql.length() - 1);

        sql.append(" where ");

        for (Column bc : beforeColumns) {
            if ("".equals(bc.getValue()) && bc.getIsNull()) {
                continue;
            }

            if (needQuote(bc.getSqlType())) {
                sql.append(bc.getName()).append("=").append("'").append(bc.getValue()).append("'");
            } else {
                sql.append(bc.getName()).append("=").append(bc.getValue());
            }

            sql.append(" and ");
        }
        sql.delete(sql.length() - 5, sql.length());
        sql.append(";");
        return sql.toString();
    }


    private static String buildInsertSql(String table, List<Column> columns) {
        if (columns.size() == 0) {
            throw new RuntimeException("Columns size is zero, can't build insert sql");
        }

        StringBuilder namesStr = new StringBuilder();
        StringBuilder valueStr = new StringBuilder();

        for (Column c : columns) {
            if ("".equals(c.getValue()) && c.getIsNull()) {
                // 跳过为空的字段，避免,,问题
                continue;
            }

            log.debug(String.format("%s, %s, sqlType: %s\n", c.getName(), c.getValue(), c.getSqlType()));

            namesStr.append(c.getName()).append(",");

            if (needQuote(c.getSqlType())) {
                valueStr.append("'").append(c.getValue()).append("'").append(",");
            } else {
                valueStr.append(c.getValue()).append(",");
            }
        }

        namesStr.deleteCharAt(namesStr.length()-1);
        valueStr.deleteCharAt(valueStr.length()-1);

        return String.format("insert into %s (%s) values (%s);", table, namesStr, valueStr);
    }

    private static String buildDeleteSql(String table, List<Column> columns) {
        if (columns.size() == 0) {
            throw new RuntimeException("Column size is zero, can't build delete sql");
        }

        StringBuilder sql = new StringBuilder(String.format("delete from %s where ", table));

        for (Column c: columns) {
            if ("".equals(c.getValue()) && c.getIsNull()) {
                continue;
            }

            log.debug(String.format("%s, %s, sqlType: %s\n", c.getName(), c.getValue(), c.getSqlType()));

            if (needQuote(c.getSqlType())) {
                sql.append(String.format("%s='%s'", c.getName(), c.getValue()));
            } else {
                sql.append(String.format("%s=%s", c.getName(), c.getValue()));
            }

            sql.append(" AND ");
        }
        sql.delete(sql.length()-5, sql.length());
        sql.append(";");

        return sql.toString();
    }
}
