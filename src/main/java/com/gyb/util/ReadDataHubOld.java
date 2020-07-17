package com.gyb.util;

import com.aliyun.datahub.DatahubClient;
import com.aliyun.datahub.DatahubConfiguration;
import com.aliyun.datahub.auth.AliyunAccount;
import com.aliyun.datahub.common.data.RecordSchema;
import com.aliyun.datahub.exception.DatahubClientException;
import com.aliyun.datahub.exception.InvalidCursorException;
import com.aliyun.datahub.model.*;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Description (测试获取datahub数据的)
 */
public class ReadDataHubOld {
    protected static final Logger logger = LoggerFactory.getLogger(ReadDataHubOld.class);
    //-----------------------------测试 公网配置-----------------------------
    private String accessId = " ";
    private String accessKey = "";
    private String endpoint = "http://dh-cn-test..com";
    private String projectName = "";
    private String topicName = "";
    private String fileName = "";
    private DatahubClient client = null;
    private String filePath = "D:\\export\\txt";
    private DatahubConfiguration conf;
    //-----------------------------测试 公网配置-----------------------------

//    private static final String  partitionkey ="TestPartitionKey";


    /**
     * 重新初始化参数
     *
     * @param accessId_
     * @param accessKey_
     * @param endpoint_
     * @param projectName_
     * @param topicName_
     * @param fileName_;
     */
    public ReadDataHubOld(String accessId_, String accessKey_, String endpoint_, String projectName_, String topicName_, String fileName_, String filePath_) {
        accessId = accessId_;
        accessKey = accessKey_;
        endpoint = endpoint_;
        projectName = projectName_;
        topicName = topicName_;
        fileName = fileName_;
        filePath = filePath_;
        AliyunAccount account = new AliyunAccount(accessId, accessKey);
        DatahubConfiguration conf = new DatahubConfiguration(account, endpoint);
        client = new DatahubClient(conf);
    }

    /**
     * 初始化datahub客户度
     */
    public ReadDataHubOld() {
        AliyunAccount account = new AliyunAccount(accessId, accessKey);
        DatahubConfiguration conf = new DatahubConfiguration(account, endpoint);
        client = new DatahubClient(conf);
    }


    /**
     * 读取高得实时数据
     */
    public void readDataHubGaoDeArray(Integer maxSize) {




        logger.info("开始获取datahub数据并产出文件");
        long start = System.currentTimeMillis();
        AliyunAccount account = new AliyunAccount(accessId, accessKey);
        DatahubConfiguration conf = new DatahubConfiguration(account, endpoint);
        DatahubClient client = new DatahubClient(conf);

        ListShardResult listShardResult = client.listShard(projectName, topicName);
        String shardId = listShardResult.getShards().get(0).getShardId();
//        GetCursorResult cursorRs = client.getCursor(projectName, topicName, shardId, GetCursorRequest.CursorType.OLDEST);
        GetCursorResult cursorRs = client.getCursor(projectName, topicName, shardId, System.currentTimeMillis()/* ms */);
        RecordSchema schema = client.getTopic(projectName, topicName).getRecordSchema();

        List<String> result = new ArrayList<>();
        //默认130000
        if (maxSize == null||maxSize<=0) {
            maxSize = 12000;
        }
        int limit = 1000;
        String cursor = cursorRs.getCursor();
        int i = 1;
        while (true) {
            try {
                GetRecordsResult recordRs = client.getRecords(projectName, topicName, shardId, cursor, limit, schema);
//                GetCursorResult recordRs = client.getCursor(projectName, topicName, shardId, System.currentTimeMillis() - 24 * 3600 * 1000 /* ms */);
                //可以获取到24小时内的第一条数据Cursor

                List<RecordEntry> recordEntries = recordRs.getRecords();

                if (recordEntries.size() == 0) {
                    if (result.size() > 0) {
                        //生成文件
                        writeDataHubData(result, fileName);
                        result.clear();

                        logger.info("获取数据至写入文件共耗时>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>" + (System.currentTimeMillis() - start)  + "毫秒");
                        start = System.currentTimeMillis();
                    }
                    // 无最新数据，请稍等重试
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                } else {
                    // curr_time   link_id  travel_time  speed  reliabilitycode  link_len adcode  time_stamp  state  public_rec_time  ds

                    if (!recordEntries.isEmpty()) {
                        for (RecordEntry rc : recordEntries) {
//                            link_id,travel_time,speed,reliabilitycode,link_len,update_time,time_stamp,adcode
                            result.add(rc.getString("link_id") + "," + rc.getString("travel_time") + "," + rc.getString("speed") + ","
                                    + rc.getString("reliabilitycode") + "," + rc.getString("link_len") + "," + rc.getString("curr_time") + ","
                                    + rc.getString("time_stamp") + "," + rc.getString("adcode"));
                            if (result.size() == maxSize) {
                                //生成文件
                                writeDataHubData(result, fileName);
                                result.clear();
                                logger.info("获取数据至写入文件共耗时>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>" + (System.currentTimeMillis() - start)  + "毫秒");
                                start = System.currentTimeMillis();
                            }
                        }
                    }

                }
                // 拿到下一个游标
                cursor = recordRs.getNextCursor();
                i++;
            } catch (InvalidCursorException ex) {
                // 非法游标或游标已过期，建议重新定位后开始消费
                cursorRs = client.getCursor(projectName, topicName, shardId, GetCursorRequest.CursorType.OLDEST);
                cursor = cursorRs.getCursor();
            } catch (DatahubClientException ex) {
                // 发生异常，需要重试
                System.out.printf(ex.getMessage());
                ex.printStackTrace();
            }
        }

    }

    /**
     * 写入txt文件
     *
     * @param result
     * @param fileName
     * @return
     */
    public boolean writeDataHubData(List<String> result, String fileName) {
        long start = System.currentTimeMillis();
        StringBuffer content = new StringBuffer();
        boolean flag = false;
        BufferedWriter out = null;
        try {
            if (result != null && !result.isEmpty() && StringUtils.isNotEmpty(fileName)) {
                fileName += "_" + DateUtils.getCurrentTime_yyyyMMddHHmmssSSS() + ".txt";
                File pathFile = new File(filePath);
                if(!pathFile.exists()){
                    pathFile.mkdirs();
                }
                String relFilePath = null;
                if(filePath.endsWith(File.separator)){
                    relFilePath = filePath + fileName;
                }else{
                    relFilePath = filePath + File.separator + fileName;
                }

                File file = new File(relFilePath);
                if (!file.exists()) {
                    file.createNewFile();
                }
                out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file), "GBK"));
//                //标题头
//                out.write("curr_time,link_id,travel_time,speed,reliabilitycode,link_len,adcode,time_stamp,state,public_rec_time,ds");
//                out.newLine();
                for (String info : result) {

                    out.write(info);
                    out.newLine();
                }
                flag = true;
                logger.info("写入文件耗时：******************" + (System.currentTimeMillis() - start) + "毫秒");
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (out != null) {
                try {
                    out.flush();
                    out.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            return flag;
        }
    }


    /**
     * 读取datahub数据测试
     */
    public void readTest() {
        AliyunAccount account = new AliyunAccount(accessId, accessKey);
        DatahubConfiguration conf = new DatahubConfiguration(account, endpoint);
        DatahubClient client = new DatahubClient(conf);

        ListShardResult listShardResult = client.listShard(projectName, topicName);
        String shardId = listShardResult.getShards().get(0).getShardId();
        GetCursorResult cursorRs = client.getCursor(projectName, topicName, shardId, GetCursorRequest.CursorType.OLDEST);
        RecordSchema schema = client.getTopic(projectName, topicName).getRecordSchema();

        int limit = 100;
        String cursor = cursorRs.getCursor();
        while (true) {
            try {
                GetRecordsResult recordRs = client.getRecords(projectName, topicName, shardId, cursor, limit, schema);
                List<RecordEntry> recordEntries = recordRs.getRecords();
                if (recordEntries.size() == 0) {
                    // 无最新数据，请稍等重试
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                } else {
                    System.out.println("------------------------");
                }
                // 拿到下一个游标
                cursor = recordRs.getNextCursor();
            } catch (InvalidCursorException ex) {
                // 非法游标或游标已过期，建议重新定位后开始消费
                cursorRs = client.getCursor(projectName, topicName, shardId, GetCursorRequest.CursorType.OLDEST);
                cursor = cursorRs.getCursor();
            } catch (DatahubClientException ex) {
                // 发生异常，需要重试
                System.out.printf(ex.getMessage());
                ex.printStackTrace();
            }
        }
    }


    public static void main(String[] args) {
//        readTest();

//        int maxSzie = 10000;
        ReadDataHubOld rf = new ReadDataHubOld();
        rf.readDataHubGaoDeArray(null);
//        readTest();

    }
}
