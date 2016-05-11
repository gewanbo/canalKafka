package com.alibaba.otter.canal.example;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.protocol.CanalEntry.*;
import com.alibaba.otter.canal.protocol.Message;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.commons.lang.SystemUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;
import java.util.Properties;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * 测试基类
 * 
 * @author jianghang 2013-4-15 下午04:17:12
 * @version 1.0.4
 */
public class AbstractCMS {

    protected final static Logger             logger             = LoggerFactory.getLogger(AbstractCMS.class);
    protected static final String             SEP                = SystemUtils.LINE_SEPARATOR;
    protected static final String             DATE_FORMAT        = "yyyy-MM-dd HH:mm:ss";
    protected volatile boolean                running            = false;
    protected Thread.UncaughtExceptionHandler handler            = new Thread.UncaughtExceptionHandler() {

                                                                     public void uncaughtException(Thread t, Throwable e) {
                                                                         logger.error("parse events has an error", e);
                                                                     }
                                                                 };
    protected Thread                          thread             = null;
    protected CanalConnector                  connector;
    protected static String                   context_format     = null;
    protected static String                   row_format         = null;
    protected static String                   transaction_format = null;
    protected String                          destination;

    protected Properties                      _config;

    protected Producer<String, String> messageProducer ;

    protected List<KeyedMessage<String, String>>  messageList        = new ArrayList<KeyedMessage<String, String>>();

    protected String                           topic;

    static {
        context_format = SEP + "****************************************************" + SEP;
        context_format += "* Batch Id: [{}] ,count : [{}] , memsize : [{}] , Time : {}" + SEP;
        context_format += "* Start : [{}] " + SEP;
        context_format += "* End : [{}] " + SEP;
        context_format += "****************************************************" + SEP;

        row_format = SEP
                     + "----------------> binlog[{}:{}] , name[{},{}] , eventType : {} , executeTime : {} , delay : {}ms"
                     + SEP;

        transaction_format = SEP + "================> binlog[{}:{}] , executeTime : {} , delay : {}ms" + SEP;

    }

    public AbstractCMS(String destination){
        this(destination, null);
    }

    public AbstractCMS(String destination, CanalConnector connector){
        this.destination = destination;
        this.connector = connector;
    }

    protected void initKafka(){
        try {
            logger.info("Start to initialize kafka connection.\n");

            String brokers = this._config.getProperty("kafka.brokers");
            this.topic     = this._config.getProperty("kafka.topic");

            logger.info("Kafka: brokers-" + brokers + " topic-" + this.topic + "\n");

            Properties props = new Properties();
            props.put("metadata.broker.list", brokers);
            props.put("serializer.class", "kafka.serializer.StringEncoder");
            props.put("request.required.acks", "1");

            ProducerConfig config = new ProducerConfig(props);

            messageProducer = new kafka.javaapi.producer.Producer<String, String>(config);

            logger.info("Kafka connection initialize has finish.\n");
        } catch (Exception e) {
            logger.error("Kafka connect Exception:", e);
        }
    }

    protected void start() {

        // Initialize kafka instance.
        initKafka();

        Assert.notNull(connector, "connector is null");
        thread = new Thread(new Runnable() {

            public void run() {
                process();
            }
        });

        thread.setUncaughtExceptionHandler(handler);
        thread.start();
        running = true;
    }

    protected void stop() {
        commitAll();

        if (!running) {
            return;
        }
        running = false;
        if (thread != null) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                // ignore
            }
        }

        MDC.remove("destination");
    }

    protected void commitAll(){

        // Commit comments list.
        if(messageList.size() > 1) {
            try {
                messageProducer.send(messageList);
                messageList = new ArrayList<KeyedMessage<String, String>>();
                messageProducer.close();
                logger.info("commit successful!");
            } catch (Exception ignore) {

            }
        }
    }

    protected void process() {
        int batchSize = 5 * 1024;
        while (running) {
            try {
                MDC.put("destination", destination);
                connector.connect();
                connector.subscribe("cmstmp01\\..*");
                while (running) {
                    Message message = connector.getWithoutAck(batchSize); // 获取指定数量的数据
                    long batchId = message.getId();
                    int size = message.getEntries().size();
                    if (batchId == -1 || size == 0) {
                        // try {
                        // Thread.sleep(1000);
                        // } catch (InterruptedException e) {
                        // }
                    } else {
                        printSummary(message, batchId, size);
                        printEntry(message.getEntries());
                    }

                    connector.ack(batchId); // 提交确认
                    // connector.rollback(batchId); // 处理失败, 回滚数据
                }
            } catch (Exception e) {
                logger.error("process error!", e);
            } finally {
                connector.disconnect();
                MDC.remove("destination");
            }
        }
    }

    private void printSummary(Message message, long batchId, int size) {
        long memsize = 0;
        for (Entry entry : message.getEntries()) {
            memsize += entry.getHeader().getEventLength();
        }

        String startPosition = null;
        String endPosition = null;
        if (!CollectionUtils.isEmpty(message.getEntries())) {
            startPosition = buildPositionForDump(message.getEntries().get(0));
            endPosition = buildPositionForDump(message.getEntries().get(message.getEntries().size() - 1));
        }

        SimpleDateFormat format = new SimpleDateFormat(DATE_FORMAT);
        logger.info(context_format, new Object[] { batchId, size, memsize, format.format(new Date()), startPosition,
                endPosition });
    }

    protected String buildPositionForDump(Entry entry) {
        long time = entry.getHeader().getExecuteTime();
        Date date = new Date(time);
        SimpleDateFormat format = new SimpleDateFormat(DATE_FORMAT);
        return entry.getHeader().getLogfileName() + ":" + entry.getHeader().getLogfileOffset() + ":"
               + entry.getHeader().getExecuteTime() + "(" + format.format(date) + ")";
    }

    protected void printEntry(List<Entry> entrys) {
        for (Entry entry : entrys) {
            long executeTime = entry.getHeader().getExecuteTime();
            long delayTime = new Date().getTime() - executeTime;
            String schemaName = entry.getHeader().getSchemaName();
            String tableName  = entry.getHeader().getTableName();

            if (entry.getEntryType() == EntryType.ROWDATA) {
                RowChange rowChage;
                try {
                    rowChage = RowChange.parseFrom(entry.getStoreValue());
                } catch (Exception e) {
                    throw new RuntimeException("parse event has an error , data:" + entry.toString(), e);
                }

                EventType eventType = rowChage.getEventType();

                logger.info(row_format,
                    new Object[] { entry.getHeader().getLogfileName(),
                            String.valueOf(entry.getHeader().getLogfileOffset()), schemaName, tableName, eventType,
                            String.valueOf(entry.getHeader().getExecuteTime()), String.valueOf(delayTime) });

                if(schemaName.equals("cmstmp01") && tableName.equals("story")) {
                    logger.info("This is a story message ------ !!!!" + SEP);
                    for (RowData rowData : rowChage.getRowDatasList()) {
                        if (eventType == EventType.DELETE) {
                            printColumn(rowData.getBeforeColumnsList());
                        } else if (eventType == EventType.INSERT) {
                            List<Column> colList = rowData.getAfterColumnsList();
                            commitStoryData(colList);
                            //printColumn(rowData.getAfterColumnsList());
                        } else {
                            printColumn(rowData.getAfterColumnsList());
                        }
                    }
                } else if (schemaName.equals("cmstmp01") && tableName.equals("comment")) {
                    logger.info("This is a comment message ------ !!!!!!" + SEP);
                    for (RowData rowData : rowChage.getRowDatasList()) {
                        if (eventType == EventType.INSERT) {
                            List<Column> colList = rowData.getAfterColumnsList();
                            commitCommentData(colList);
                            //printColumn(colList);
                        }
                    }
                } else if (schemaName.equals("cmstmp01") && tableName.equals("userfavorites")) {
                    logger.info("This is a favorite story message ------ !!!!!!" + SEP);
                    for (RowData rowData : rowChage.getRowDatasList()) {
                        if (eventType == EventType.INSERT) {
                            List<Column> colList = rowData.getAfterColumnsList();
                            commitFavoriteStory(colList);
                            //printColumn(colList);
                        }
                    }
                }
            }
        }
    }

    protected void printColumn(List<Column> columns) {
        for (Column column : columns) {
            StringBuilder builder = new StringBuilder();
            builder.append(column.getName()).append(" : ").append(column.getValue());
            builder.append("    type=").append(column.getMysqlType());
            if (column.getUpdated()) {
                builder.append("    update=").append(column.getUpdated());
            }
            builder.append(SEP);
            logger.info(builder.toString());
        }
    }

    protected void commitStoryData(List<Column> columns) {

        JSONObject msgObj = new JSONObject();
        msgObj.put("__messageType", "story");

        logger.info("Starting to initialize comment message." + SEP);

        // One row
        for (Column column : columns) {
            StringBuilder builder = new StringBuilder();
            builder.append(column.getName()).append(" : ").append(column.getValue());
            builder.append("    type=").append(column.getMysqlType());
            if (column.getUpdated()) {
                builder.append("    update=").append(column.getUpdated());
            }
            builder.append(SEP);
            logger.info(builder.toString() + SEP);

            if(column.getName().equals("cheadline")) {
                msgObj.put("cheadline", column.getValue());
            } else if (column.getName().equals("id")) {
                msgObj.put("storyid", column.getValue());
            } else if (column.getName().equals("cbody")) {
                msgObj.put("cbody", column.getValue());
            } else if (column.getName().equals("tag")) {
                msgObj.put("tag", column.getValue());
            }
        }

        logger.info(msgObj.toJSONString() + SEP);

        messageList.add(new KeyedMessage<String, String>(this.topic, msgObj.toJSONString()));

        if(messageList.size() > 1) {
            try {
                messageProducer.send(messageList);
                messageList = new ArrayList<KeyedMessage<String, String>>();
                logger.info("Comment message committed successful!" + SEP);
            } catch (Exception e) {
                logger.error("Throws exception when send message to kafka:", e);
            }
        }
    }

    protected void commitCommentData(List<Column> columns) {

        JSONObject msgObj = new JSONObject();
        msgObj.put("__messageType", "comments");

        logger.info("Starting to initialize comment message." + SEP);

        // One row
        for (Column column : columns) {
            StringBuilder builder = new StringBuilder();
            builder.append(column.getName()).append(" : ").append(column.getValue());
            builder.append("    type=").append(column.getMysqlType());
            if (column.getUpdated()) {
                builder.append("    update=").append(column.getUpdated());
            }
            builder.append(SEP);
            logger.info(builder.toString());

            if(column.getName().equals("userid")) {
                msgObj.put("uuid", column.getValue());
            } else if (column.getName().equals("storyid")) {
                msgObj.put("storyid", column.getValue());
            } else if (column.getName().equals("dnewdate")) {
                String dateStr = column.getValue();
                int t = 0;
                try {
                    t = (int)(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(dateStr).getTime() / 1000);
                } catch (Exception ignore) {}

                msgObj.put("datetime", t);
            }
        }

        logger.info(msgObj.toJSONString() + SEP);

        messageList.add(new KeyedMessage<String, String>(this.topic, msgObj.toJSONString()));

        if(messageList.size() > 1) {
            try {
                messageProducer.send(messageList);
                messageList = new ArrayList<KeyedMessage<String, String>>();
                logger.info("Comment message committed successful!" + SEP);
            } catch (Exception e) {
                logger.error("Throws exception when send message to kafka:", e);
            }
        }
    }

    protected void commitFavoriteStory(List<Column> columns) {

        JSONObject msgObj = new JSONObject();
        msgObj.put("__messageType", "favorite_story");

        logger.info("Starting to initialize comment message." + SEP);

        // One row
        for (Column column : columns) {
            StringBuilder builder = new StringBuilder();
            builder.append(column.getName()).append(" : ").append(column.getValue());
            builder.append("    type=").append(column.getMysqlType());
            if (column.getUpdated()) {
                builder.append("    update=").append(column.getUpdated());
            }
            builder.append(SEP);
            logger.info(builder.toString());

            if(column.getName().equals("userid")) {
                msgObj.put("uuid", column.getValue());
            } else if (column.getName().equals("storyid")) {
                msgObj.put("storyid", column.getValue());
            } else if (column.getName().equals("dnewdate")) {
                String dateStr = column.getValue();
                int t = 0;
                try {
                    t = (int)(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(dateStr).getTime() / 1000);
                } catch (Exception ignore) {}

                msgObj.put("datetime", t);
            }
        }

        logger.info(msgObj.toJSONString() + SEP);

        messageList.add(new KeyedMessage<String, String>(this.topic, msgObj.toJSONString()));

        if(messageList.size() > 1) {
            try {
                messageProducer.send(messageList);
                messageList = new ArrayList<KeyedMessage<String, String>>();
                logger.info("Comment message committed successful!" + SEP);
            } catch (Exception e) {
                logger.error("Throws exception when send message to kafka:", e);
            }
        }
    }

    public void setConnector(CanalConnector connector) {
        this.connector = connector;
    }

    public void setConfig(Properties conf){
        this._config = conf;
    }

}
