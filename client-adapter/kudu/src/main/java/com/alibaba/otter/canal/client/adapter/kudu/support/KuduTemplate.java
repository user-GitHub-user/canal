package com.alibaba.otter.canal.client.adapter.kudu.support;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @author liuyadong
 * @description kudu 操作工具类
 */
public class KuduTemplate {

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    private KuduClient kuduClient;
    private String masters;

    private final static int OPERATION_BATCH = 500;

    private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    
    private SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHH");
    
    private static String today = "2020041311";
    
    private static ArrayList<String> todayTable = new ArrayList<>();

    public KuduTemplate(String master_str) {
        this.masters = master_str;
        checkClient();
    }

    /**
     * 检测连接
     */
    private void checkClient() {
        if (kuduClient == null) {
            //kudu master 以逗号分隔
            List<String> masterList = Arrays.asList(masters.split(","));
            kuduClient = new KuduClient.KuduClientBuilder(masterList)
                    .defaultOperationTimeoutMs(60000)
                    .defaultSocketReadTimeoutMs(60000)
                    .defaultAdminOperationTimeoutMs(60000).build();
        }
    }

    /**
     * 查询表是否存在
     *
     * @param tableName
     * @return
     */
    public boolean tableExists(String tableName) {
        this.checkClient();
        try {
            return kuduClient.tableExists(tableName);
        } catch (KuduException e) {
            logger.error("kudu table exists check fail,message :{}", e.getMessage());
            return true;
        }
    }

    /**
     * 删除行，错误后重试
     *
     * @param tableName
     * @param dataList
     * @throws KuduException
     */
    public void delete(String tableName, List<Map<String, Object>> dataList) throws KuduException {
        boolean haveError = false;
        this.checkClient();
        KuduTable kuduTable = kuduClient.openTable(tableName);
        KuduSession session = kuduClient.newSession();
        session.setTimeoutMillis(60000);
        Date date = new Date();
        if (!format.format(date).equals(today)) {
            today = format.format(date);
            todayTable.clear();
        }
        if(!todayTable.contains(tableName)) {
            logger.info("dsa_canal_info table DML :{},{}", date.getTime(), tableName);
            todayTable.add(tableName);
        }
        try {
            session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);
            session.setMutationBufferSpace(OPERATION_BATCH);
            //获取元数据结构
            Map<String, Type> metaMap = new HashMap<>();
            Schema schema = kuduTable.getSchema();
            for (ColumnSchema columnSchema : schema.getColumns()) {
                String colName = columnSchema.getName().toLowerCase();
                Type type = columnSchema.getType();
                metaMap.put(colName, type);
            }
            int uncommit = 0;
            for (Map<String, Object> data : dataList) {
                Delete delete = kuduTable.newDelete();
                PartialRow row = delete.getRow();
                for (Map.Entry<String, Object> entry : data.entrySet()) {
                    String name = entry.getKey().toLowerCase();
                    Type type = metaMap.get(name);
                    if(type == null) {
                        continue;
                    }
                    Object value = entry.getValue();
                    fillRow(row, name, value, type); //填充行数据
                }
                session.apply(delete);
                // 对于手工提交, 需要buffer在未满的时候flush,这里采用了buffer一半时即提交
                uncommit = uncommit + 1;
                if (uncommit > OPERATION_BATCH / 3 * 2) {
                    List<OperationResponse> delete_option = session.flush();
                    if (delete_option.size() > 0) {
                        OperationResponse response = delete_option.get(0);
                        if (response.hasRowError()) {
                            String error = response.getRowError().getMessage();
                            if (error != null && error.contains("key not found")) {
                                logger.warn("delete row fail table name is :{} ", tableName);
                                logger.warn("error list is :{}", error);
                            } else {
                                logger.warn("delete row fail table name is :{} ", tableName);
                                logger.warn("error list is :{}", error);
                                haveError = true;
                            }
                        }
                    }
                    uncommit = 0;
                }
            }
            List<OperationResponse> delete_option = session.flush();
            if (delete_option.size() > 0) {
                OperationResponse response = delete_option.get(0);
                if (response.hasRowError()) {
                    String error = response.getRowError().getMessage();
                    if (error != null && error.contains("key not found")) {
                        logger.warn("delete row fail table name is :{} ", tableName);
                        logger.warn("error list is :{}", error);
                    } else {
                        logger.warn("delete row fail table name is :{} ", tableName);
                        logger.warn("error list is :{}", error);
                        haveError = true;
                    }
                }
                
            }

        } catch (KuduException e) {
            logger.error("error message is :{}", dataList.toString());
            throw e;
        } finally {
            if (!session.isClosed()) {
                session.close();
            }
            if(haveError) {
                logger.info("elephant_wang info：delete retry {}" ,tableName);
                deleteRetry(tableName, dataList, 30);
            }
        }
    }
    
    /**
     * 删除行,不会重试
     *
     * @param tableName
     * @param dataList
     * @throws KuduException
     */
    public void deleteRetry(String tableName, List<Map<String, Object>> dataList, int times) throws KuduException {
        //每重试3次未成功，重新连接kudu
        if(times%3 == 0){
            reConnectKuduClient();
        }
        //重试5次不成功，打印一次error
        if(times == 5){
            logger.error("deleteRetry 5 times row fail table name is :{} ", tableName);
        }
        boolean haveError = false;
        this.checkClient();
        KuduTable kuduTable = kuduClient.openTable(tableName);
        KuduSession session = kuduClient.newSession();
        session.setTimeoutMillis(60000);
        try {
            session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);
            session.setMutationBufferSpace(OPERATION_BATCH);
            //获取元数据结构
            Map<String, Type> metaMap = new HashMap<>();
            Schema schema = kuduTable.getSchema();
            for (ColumnSchema columnSchema : schema.getColumns()) {
                String colName = columnSchema.getName().toLowerCase();
                Type type = columnSchema.getType();
                metaMap.put(colName, type);
            }
            int uncommit = 0;
            for (Map<String, Object> data : dataList) {
                Delete delete = kuduTable.newDelete();
                PartialRow row = delete.getRow();
                for (Map.Entry<String, Object> entry : data.entrySet()) {
                    String name = entry.getKey().toLowerCase();
                    Type type = metaMap.get(name);
                    if(type == null) {
                        continue;
                    }
                    Object value = entry.getValue();
                    fillRow(row, name, value, type); //填充行数据
                }
                session.apply(delete);
                // 对于手工提交, 需要buffer在未满的时候flush,这里采用了buffer一半时即提交
                uncommit = uncommit + 1;
                if (uncommit > OPERATION_BATCH / 3 * 2) {
                    List<OperationResponse> delete_option = session.flush();
                    if (delete_option.size() > 0) {
                        OperationResponse response = delete_option.get(0);
                        if (response.hasRowError()) {
                            String error = response.getRowError().getMessage();
                            if (error != null && error.contains("key not found")) {
                                logger.warn("deleteRetry row fail table name is :{} ", tableName);
                                logger.warn("deleteRetry error list is :{}", error);
                            } else if (times > 1) {
                                logger.warn("deleteRetry row fail table name is :{} ", tableName);
                                logger.warn("deleteRetry error list is :{}", error);
                                haveError = true;
                            } else {
                                logger.error("deleteRetry row fail table name is :{} ", tableName);
                                logger.error("deleteRetry error list is :{}", error);
                                haveError = true;
                            }
                        }
                    }
                    uncommit = 0;
                }
            }
            List<OperationResponse> delete_option = session.flush();
            if (delete_option.size() > 0) {
                OperationResponse response = delete_option.get(0);
                if (response.hasRowError()) {
                    String error = response.getRowError().getMessage();
                    if (error != null && error.contains("key not found")) {
                        logger.warn("deleteRetry row fail table name is :{} ", tableName);
                        logger.warn("deleteRetry error list is :{}", error);
                    } else if (times > 1) {
                        logger.warn("deleteRetry row fail table name is :{} ", tableName);
                        logger.warn("deleteRetry error list is :{}", error);
                        haveError = true;
                    } else {
                        logger.error("deleteRetry row fail table name is :{} ", tableName);
                        logger.error("deleteRetry error list is :{}", error);
                        haveError = true;
                    }
                }
            }

        } catch (KuduException e) {
            logger.error("deleteRetry error message is :{}", dataList.toString());
            throw e;
        } finally {
            if (!session.isClosed()) {
                session.close();
            }
            logger.info("elephant_wang info：deleteRetry{} table name is :{}" , times, tableName);
            if(haveError && --times > 0) {
                deleteRetry(tableName, dataList, times);
            }
        }
    }
    
    
    /**
     * 清空表，错误后重试
     *
     * @param tableName
     * @param pkIds 
     * @param dataList
     * @throws KuduException
     */
    public void truncate(String tableName, List<String> pkIds) throws KuduException {
        this.checkClient();
        Date date = new Date();
        if (!format.format(date).equals(today)) {
            today = format.format(date);
            todayTable.clear();
        }
        if(!todayTable.contains(tableName)) {
            logger.info("dsa_canal_info table DML :{},{}", date.getTime(), tableName);
            todayTable.add(tableName);
        }
        try {
            KuduTable kuduTable = kuduClient.openTable(tableName);
            String bakTableName = kuduTable.getName() + "_elephantwang_bak";
            PartitionSchema ps =  kuduTable.getPartitionSchema();
            CreateTableOptions builder = new CreateTableOptions();
            if(!ps.getHashBucketSchemas().isEmpty()) {
                builder.addHashPartitions(pkIds, ps.getHashBucketSchemas().get(0).getNumBuckets());
            }
            kuduClient.createTable(bakTableName,kuduTable.getSchema() ,builder);
            kuduClient.deleteTable(tableName);
            logger.info("truncate tableName exists :{}", kuduClient.tableExists(tableName));
            kuduClient.alterTable(bakTableName, new AlterTableOptions().renameTable(tableName));
           boolean done =  kuduClient.isAlterTableDone(tableName);
           if (!done) {
               logger.error("truncate not done message is tableName :{}", tableName);
           }
        } catch (KuduException e) {
            logger.error("truncate error tableName is :{}", tableName);
            throw e;
        }
    }
    
    /**
     * 更新/插入字段，失败重试
     *
     * @param tableName
     * @param dataList
     * @throws KuduException
     */
    public void upsert(String tableName, List<Map<String, Object>> dataList) throws KuduException {
        boolean haveError = false;
        this.checkClient();
        KuduTable kuduTable = kuduClient.openTable(tableName);
        KuduSession session = kuduClient.newSession();
        session.setTimeoutMillis(60000);
        Date date = new Date();
        if (!format.format(date).equals(today)) {
            today = format.format(date);
            todayTable.clear();
        }
        if(!todayTable.contains(tableName)) {
            logger.info("dsa_canal_info table DML :{},{}", date.getTime(), tableName);
            todayTable.add(tableName);
        }
        try {
            session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);
            session.setMutationBufferSpace(OPERATION_BATCH);
            //获取元数据结构
            Map<String, Type> metaMap = new HashMap<>();
            Schema schema = kuduTable.getSchema();
            for (ColumnSchema columnSchema : schema.getColumns()) {
                String colName = columnSchema.getName().toLowerCase();
                Type type = columnSchema.getType();
                metaMap.put(colName, type);
            }
            int uncommit = 0;
            for (Map<String, Object> data : dataList) {
                Upsert upsert = kuduTable.newUpsert();
                PartialRow row = upsert.getRow();
                for (Map.Entry<String, Object> entry : data.entrySet()) {
                    String name = entry.getKey().toLowerCase();
                    Type type = metaMap.get(name);
                    if(type == null) {
                        continue;
                    }
                    Object value = entry.getValue();
                    fillRow(row, name, value, type); //填充行数据
                }
                session.apply(upsert);
                // 对于手工提交, 需要buffer在未满的时候flush,这里采用了buffer一半时即提交
                uncommit = uncommit + 1;
                if (uncommit > OPERATION_BATCH / 3 * 2) {
                    List<OperationResponse> update_option = session.flush();
                    if (update_option.size() > 0) {
                        OperationResponse response = update_option.get(0);
                        if (response.hasRowError()) {
                            logger.warn("update row fail table name is :{} ", tableName);
                            logger.warn("update list is :{}", response.getRowError().getMessage());
                            haveError = true;
                        }
                    }
                    uncommit = 0;
                }
            }
            List<OperationResponse> update_option = session.flush();
            if (update_option.size() > 0) {
                OperationResponse response = update_option.get(0);
                if (response.hasRowError()) {
                    logger.warn("update row fail table name is :{} ", tableName);
                    logger.warn("update list is :{}", response.getRowError().getMessage());
                    haveError = true;
                }
            }
        } catch (KuduException e) {
            logger.error("error message is :{}", dataList.toString());
            throw e;
        } finally {
            if (!session.isClosed()) {
                session.close();
            }
            if (haveError) {
                logger.info("elephant_wang info：update retry {}" ,tableName);
                upsertRetry(tableName, dataList,  30);
            }
        }
    }
    
    /**
     * 更新/插入字段,不会重试
     *
     * @param tableName
     * @param dataList
     * @throws KuduException
     */
    public void upsertRetry(String tableName, List<Map<String, Object>> dataList, int times) throws KuduException {
        //每重试3次未成功，重新连接kudu
        if(times%3 == 0){
            reConnectKuduClient();
        }
        //重试5次不成功，打印一次error
        if(times == 5){
            logger.error("upsertRetry 5 times row fail table name is :{} ", tableName);
        }
        boolean haveError = false;
        this.checkClient();
        KuduTable kuduTable = kuduClient.openTable(tableName);
        KuduSession session = kuduClient.newSession();
        session.setTimeoutMillis(60000);
        try {
            session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);
            session.setMutationBufferSpace(OPERATION_BATCH);
            //获取元数据结构
            Map<String, Type> metaMap = new HashMap<>();
            Schema schema = kuduTable.getSchema();
            for (ColumnSchema columnSchema : schema.getColumns()) {
                String colName = columnSchema.getName().toLowerCase();
                Type type = columnSchema.getType();
                metaMap.put(colName, type);
            }
            int uncommit = 0;
            for (Map<String, Object> data : dataList) {
                Upsert upsert = kuduTable.newUpsert();
                PartialRow row = upsert.getRow();
                for (Map.Entry<String, Object> entry : data.entrySet()) {
                    String name = entry.getKey().toLowerCase();
                    Type type = metaMap.get(name);
                    if(type == null) {
                        continue;
                    }
                    Object value = entry.getValue();
                    fillRow(row, name, value, type); //填充行数据
                }
                session.apply(upsert);
                // 对于手工提交, 需要buffer在未满的时候flush,这里采用了buffer一半时即提交
                uncommit = uncommit + 1;
                if (uncommit > OPERATION_BATCH / 3 * 2) {
                    List<OperationResponse> update_option = session.flush();
                    if (update_option.size() > 0) {
                        OperationResponse response = update_option.get(0);
                        if (response.hasRowError()) {
                            if(times > 1) {
                                logger.warn("upsertRetry row fail table name is :{} ", tableName);
                                logger.warn("upsertRetry list is :{}", response.getRowError().getMessage());
                            } else {
                                logger.error("upsertRetry row fail table name is :{} ", tableName);
                                logger.error("upsertRetry list is :{}", response.getRowError().getMessage());
                            }
                            haveError = true;
                        }
                    }
                    uncommit = 0;
                }
            }
            List<OperationResponse> update_option = session.flush();
            if (update_option.size() > 0) {
                OperationResponse response = update_option.get(0);
                if (response.hasRowError()) {
                    if(times > 1) {
                        logger.warn("upsertRetry row fail table name is :{} ", tableName);
                        logger.warn("upsertRetry list is :{}", response.getRowError().getMessage());
                    } else {
                        logger.error("upsertRetry row fail table name is :{} ", tableName);
                        logger.error("upsertRetry list is :{}", response.getRowError().getMessage());
                    }
                    haveError = true;
                }
            }
        } catch (KuduException e) {
            logger.error("upsertRetry error message is :{}", dataList.toString());
            throw e;
        } finally {
            if (!session.isClosed()) {
                session.close();
            }
            logger.info("elephant_wang info：upsertRetry{} table name is :{}" , times, tableName);
            if(haveError && --times > 0) {
                upsertRetry(tableName, dataList, times);
            }
        }
    }

    /**
     * 插入数据,失败重复一次
     *
     * @param tableName
     * @param dataList
     * @throws KuduException
     */
    public void insert(String tableName, List<Map<String, Object>> dataList) throws KuduException {
        boolean haveError = false;
        this.checkClient();
        KuduTable kuduTable = kuduClient.openTable(tableName);// 打开表
        KuduSession session = kuduClient.newSession();  // 创建写session,kudu必须通过session写入
        session.setTimeoutMillis(60000);
        Date date = new Date();
        if (!format.format(date).equals(today)) {
            today = format.format(date);
            todayTable.clear();
        }
        if(!todayTable.contains(tableName)) {
            logger.info("dsa_canal_info table DML :{},{}", date.getTime(), tableName);
            todayTable.add(tableName);
        }
        try {
            session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH); // 采取Flush方式 手动刷新
            session.setMutationBufferSpace(OPERATION_BATCH);
            //获取元数据结构
            Map<String, Type> metaMap = new HashMap<>();
            Schema schema = kuduTable.getSchema();
            for (ColumnSchema columnSchema : schema.getColumns()) {
                String colName = columnSchema.getName().toLowerCase();
                Type type = columnSchema.getType();
                metaMap.put(colName, type);
            }
            int uncommit = 0;
            for (Map<String, Object> data : dataList) {
                Insert insert = kuduTable.newInsert();
                PartialRow row = insert.getRow();
                for (Map.Entry<String, Object> entry : data.entrySet()) {
                    String name = entry.getKey().toLowerCase();
                    Type type = metaMap.get(name);
                    if(type == null) {
                        continue;
                    }
                    Object value = entry.getValue();
                    fillRow(row, name, value, type); //填充行数据
                }
                session.apply(insert);
                // 对于手工提交, 需要buffer在未满的时候flush,这里采用了buffer一半时即提交
                uncommit = uncommit + 1;
                if (uncommit > OPERATION_BATCH / 3 * 2) {
                    List<OperationResponse> insert_option = session.flush();
                    if (insert_option.size() > 0) {
                        OperationResponse response = insert_option.get(0);
                        if (response.hasRowError()) {
                            String error = response.getRowError().getMessage();
                            if (error != null && error.contains("key already present")) {
                                logger.warn("insert row fail table name is :{} ", tableName);
                                logger.warn("insert list is :{}", error);
                            } else {
                                logger.warn("insert row fail table name is :{} ", tableName);
                                logger.warn("insert list is :{}", error);
                                haveError = true;
                            }
                        }
                    }
                    uncommit = 0;
                }
            }
            List<OperationResponse> insert_option = session.flush();
            if (insert_option.size() > 0) {
                OperationResponse response = insert_option.get(0);
                if (response.hasRowError()) {
                    String error = response.getRowError().getMessage();
                    if (error != null && error.contains("key already present")) {
                        logger.warn("insert row fail table name is :{} ", tableName);
                        logger.warn("insert list is :{}", error);
                    } else {
                        logger.warn("insert row fail table name is :{} ", tableName);
                        logger.warn("insert list is :{}", error);
                        haveError = true;
                    }
                }
            }
        } catch (KuduException e) {
            logger.error("error message is :{}", dataList.toString());
            throw e;
        } finally {
            if (!session.isClosed()) {
                session.close();
            }
            if (haveError) {
                logger.info("elephant_wang info：insert retry {}" ,tableName);
                insertRetry(tableName, dataList, 30);
            }
        }

    }

    
    /**
     * 插入数据,不会重试
     *
     * @param tableName
     * @param dataList
     * @throws KuduException
     */
    public void insertRetry(String tableName, List<Map<String, Object>> dataList, int times) throws KuduException {
        //每重试3次未成功，重新连接kudu
        if(times%3 == 0){
            reConnectKuduClient();
        }
        //重试5次不成功，打印一次error
        if(times == 5){
            logger.error("insertRetry 5 times row fail table name is :{} ", tableName);
        }
        boolean haveError = false;
        this.checkClient();
        KuduTable kuduTable = kuduClient.openTable(tableName);// 打开表
        KuduSession session = kuduClient.newSession();  // 创建写session,kudu必须通过session写入
        session.setTimeoutMillis(60000);
        try {
            session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH); // 采取Flush方式 手动刷新
            session.setMutationBufferSpace(OPERATION_BATCH);
            //获取元数据结构
            Map<String, Type> metaMap = new HashMap<>();
            Schema schema = kuduTable.getSchema();
            for (ColumnSchema columnSchema : schema.getColumns()) {
                String colName = columnSchema.getName().toLowerCase();
                Type type = columnSchema.getType();
                metaMap.put(colName, type);
            }
            int uncommit = 0;
            for (Map<String, Object> data : dataList) {
                Insert insert = kuduTable.newInsert();
                PartialRow row = insert.getRow();
                for (Map.Entry<String, Object> entry : data.entrySet()) {
                    String name = entry.getKey().toLowerCase();
                    Type type = metaMap.get(name);
                    if(type == null) {
                        continue;
                    }
                    Object value = entry.getValue();
                    fillRow(row, name, value, type); //填充行数据
                }
                session.apply(insert);
                // 对于手工提交, 需要buffer在未满的时候flush,这里采用了buffer一半时即提交
                uncommit = uncommit + 1;
                if (uncommit > OPERATION_BATCH / 3 * 2) {
                    List<OperationResponse> insert_option = session.flush();
                    if (insert_option.size() > 0) {
                        OperationResponse response = insert_option.get(0);
                        if (response.hasRowError()) {
                            String error = response.getRowError().getMessage();
                            if (error != null && error.contains("key already present")) {
                                logger.warn("insertRetry row fail table name is :{} ", tableName);
                                logger.warn("insertRetry list is :{}", error);
                            } else if(times > 1) {
                                logger.warn("insertRetry row fail table name is :{} ", tableName);
                                logger.warn("insertRetry list is :{}", error);
                                haveError = true;
                            } else {
                                logger.error("insertRetry row fail table name is :{} ", tableName);
                                logger.error("insertRetry list is :{}", error);
                                haveError = true;
                            }
                        }
                    }
                    uncommit = 0;
                }
            }
            List<OperationResponse> insert_option = session.flush();
            if (insert_option.size() > 0) {
                OperationResponse response = insert_option.get(0);
                if (response.hasRowError()) {
                    String error = response.getRowError().getMessage();
                    if (error != null && error.contains("key already present")) {
                        logger.warn("insertRetry row fail table name is :{} ", tableName);
                        logger.warn("insertRetry list is :{}", error);
                    } else if(times > 1) {
                        logger.warn("insertRetry row fail table name is :{} ", tableName);
                        logger.warn("insertRetry list is :{}", error);
                        haveError = true;
                    } else {
                        logger.error("insertRetry row fail table name is :{} ", tableName);
                        logger.error("insertRetry list is :{}", error);
                        haveError = true;
                    }
                }
            }
        } catch (KuduException e) {
            logger.error("insertRetry error message is :{}", dataList.toString());
            throw e;
        } finally {
            if (!session.isClosed()) {
                session.close();
            }
            logger.info("elephant_wang info：insertRetry{} table name is :{}" , times, tableName);
            if(haveError && --times > 0) {
                insertRetry(tableName, dataList, times);
            }
        }
    }
    
    /**
     * 统计kudu表数据
     *
     * @param tableName
     * @return
     */
    public long countRow(String tableName) {
        this.checkClient();
        long rowCount = 0L;
        try {
            KuduTable kuduTable = kuduClient.openTable(tableName);
            //创建scanner扫描
            KuduScanner scanner = kuduClient.newScannerBuilder(kuduTable).build();
            //遍历数据
            while (scanner.hasMoreRows()) {
                while (scanner.nextRows().hasNext()) {
                    rowCount++;
                }
            }
        } catch (KuduException e) {
            e.printStackTrace();
        }
        return rowCount;
    }

    /**
     * 关闭钩子
     *
     * @throws IOException
     */
    public void closeKuduClient() {
        if (kuduClient != null) {
            try {
                kuduClient.close();
            } catch (Exception e) {
                logger.error("ShutdownHook Close KuduClient Error! error message {}", e.getMessage());
            }
        }
    }
    
    /**
     * 重新连接kudu
     *
     * @throws IOException
     */
    public void reConnectKuduClient() {
        if (kuduClient != null) {
            try {
                kuduClient.close();
                kuduClient = null;
            } catch (Exception e) {
                logger.error("ShutdownHook Close KuduClient Error! error message {}", e.getMessage());
            }
        }
        checkClient();
    }

    /**
     * 封装kudu行数据
     *
     * @param row
     * @param rawVal
     * @param type
     */
    private void fillRow(PartialRow row, String colName, Object rawVal, Type type) {
        if (type == null) {
            logger.warn("got unknown type {} for column '{}'-- ignoring this column", type, colName);
            return;
        }
        String rowValue = "0";
        if (rawVal == null) {
            row.setNull(colName);
        } else {
            rowValue = rawVal + "";
        }
        try {
            switch (type) {
                case INT8:
                    row.addByte(colName, Byte.parseByte(rowValue));
                    break;
                case INT16:
                    row.addShort(colName, Short.parseShort(rowValue));
                    break;
                case INT32:
                    row.addInt(colName, Integer.parseInt(rowValue));
                    break;
                case INT64:
                    row.addLong(colName, Long.parseLong(rowValue));
                    break;
                case BINARY:
                    row.addBinary(colName, rowValue.getBytes());
                    break;
                case STRING:
                    row.addString(colName, rowValue);
                    break;
                case BOOL:
                    if (!("true".equalsIgnoreCase(rowValue) || "false".equalsIgnoreCase(rowValue))) {
                        return;
                    }
                    row.addBoolean(colName, Boolean.parseBoolean(rowValue));
                    break;
                case FLOAT:
                    row.addFloat(colName, Float.parseFloat(rowValue));
                    break;
                case DOUBLE:
                    row.addDouble(colName, Double.parseDouble(rowValue));
                    break;
                case UNIXTIME_MICROS:
                    if ("0".equals(rowValue)) {
                        try {
                            Date parse = sdf.parse("2099-11-11 11:11:11");
                            row.addLong(colName, parse.getTime());
                        } catch (ParseException e) {
                            logger.warn("date column is null");
                        }
                    } else {
                        try {
                            Date parse = rowValue.length() > 19 ? sdf.parse(rowValue.substring(0, 19)) : sdf.parse(rowValue);
                            row.addLong(colName, parse.getTime());
                        } catch (ParseException e) {
                            logger.warn("date format error, error data is :{}", rowValue);
                            try {
                                Date parse = sdf.parse("2099-11-11 11:11:11");
                                row.addLong(colName, parse.getTime());
                            } catch (ParseException ie) {
                                logger.warn("date column is null");
                            }
                        }
                    }
                    break;
                default:
                    logger.warn("got unknown type {} for column '{}'-- ignoring this column", type, colName);
            }
        } catch (NumberFormatException e) {
            logger.error(e.getMessage());
            logger.error("column parse fail==> column name=>{},column type=>{},column value=>{}", colName, type, rawVal);
        }
    }

    /**
     * kudu数据类型映射
     *
     * @param
     */
    private Type toKuduType(String mysqlType) throws IllegalArgumentException {

        switch (mysqlType) {
            case "varchar":
                return Type.STRING;
            case "int":
                return Type.INT8;
            case "decimal":
                return Type.DOUBLE;
            case "double":
                return Type.DOUBLE;
            case "datetime":
                return Type.STRING;
            case "timestamp":
                return Type.STRING;
            default:
                throw new IllegalArgumentException("The provided data type doesn't map to know any known one.");
        }
    }
}
