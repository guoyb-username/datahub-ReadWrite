package com.gyb.controller;

import com.aliyun.datahub.DatahubClient;
import com.aliyun.datahub.DatahubConfiguration;
import com.aliyun.datahub.auth.AliyunAccount;
import com.aliyun.datahub.exception.*;
import com.aliyun.datahub.model.GetCursorRequest.CursorType;
import com.aliyun.datahub.model.*;
import com.gyb.util.TextUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * Description (通过订阅消息方式从datahub里获取数据并生成文件)
 */
public class ReadDataHub {

    protected static final Logger logger = LoggerFactory.getLogger(ReadDataHub.class);

    //-----------------------------公网配置-----------------------------
    private DatahubClient client = null;
    private String accessId = " ";
    private String accessKey = " ";
    //datahub服务地址
    private String endpoint = "http://dh-cn- .aliyuncs.com";
//    private String projectName = " ";
    private String projectName = " ";

//    private String topicName = " ";
    private String topicName = " ";

    //订阅id
//    private String subId = " ";
    private String subId = " ";
    //2018-03-07 测试订阅id
//    private String subId = "1520391035993tSRdc";


    private DatahubConfiguration conf;


    //要生成的文件名称
//    private String fileName = " ";
    private String fileName = " ";
    //要生成的文件地址
   //private String filePath = "D:\\export\\txt";
    String filePath = File.separator + "home" + File.separator + "hzjj";//产出文件的的路径
//    String filePath = File.separator+"home"+File.separator+"gaoxueyong"+File.separator+"server"+File.separator+"hzjj";//产出文件的的路径
//		String filePath = File.separator+"home"+File.separator+"test";//产出文件的的路径
    //-----------------------------公网配置-----------------------------


     private String common = "|$|";

    //初始化
    public ReadDataHub() {
        this.conf = new DatahubConfiguration(new AliyunAccount(accessId, accessKey), endpoint);
        this.client = new DatahubClient(conf);
    }


    //    (accessId, accessKey, endpoint, projectName, topicName, fileName,filePath)
    //初始化
    public ReadDataHub(String accessId_, String accessKey_, String endpoint_, String projectName_, String topicName_,
                       String fileName_, String filePath_, String subId_) {
        accessId = accessId_;
        accessKey = accessKey_;
        endpoint = endpoint_;
        projectName = projectName_;
        topicName = topicName_;
        fileName = fileName_;
        filePath = filePath_;
        subId = subId_;
        this.conf = new DatahubConfiguration(new AliyunAccount(accessId, accessKey), endpoint);
        this.client = new DatahubClient(conf);
    }


    /**
     * 每个文件的最大数据条数
     *
     * @param maxSize
     */
    public void start(Integer maxSize, String shardId) throws Exception {

        long start = System.currentTimeMillis();
        List<String> result = new ArrayList<>();
        //默认130000
        if (maxSize == null || maxSize <= 0) {
            maxSize = 30000;
        }

        boolean bExit = false;
        GetTopicResult topicResult = client.getTopic(projectName, topicName);
        // 首先初始化offset上下文
        OffsetContext offsetCtx = client.initOffsetContext(projectName, topicName, subId, shardId);
        String cursor = null; // 开始消费的cursor
        if (!offsetCtx.hasOffset()) {
            // 之前没有存储过点位，先获取初始点位，比如这里获取当前该shard最早的数据
            GetCursorResult cursorResult = client.getCursor(projectName, topicName, shardId, CursorType.OLDEST);
//            GetCursorResult cursorResult = client.getCursor(projectName, topicName, shardId, System.currentTimeMillis() - 24 * 3600 * 1000 /* ms */);

            cursor = cursorResult.getCursor();

        } else {
            // 否则，获取当前已消费点位的下一个cursor

            try{
                cursor = client.getNextOffsetCursor(offsetCtx).getCursor();
            }catch (Exception ex){
                //存在无效的点位  调过无效的点位 跳至当前订阅有效点位最开始的时刻
                if(ex.getMessage().indexOf("Seek Out of Range")>0){
                    GetCursorResult cursorResult = client.getCursor(projectName, topicName, shardId, CursorType.OLDEST);
                    cursor = cursorResult.getCursor();
                }
            }
        }
//        logger.info("Start consume records, begin offset context:" + offsetCtx.toObjectNode().toString()
//                + ", cursor:" + cursor);

        long recordNum = 0L;
        int limit = 30000;
        while (!bExit) {
            try {
                GetRecordsResult recordResult = client.getRecords(projectName, topicName, shardId, cursor, limit,
                        topicResult.getRecordSchema());

                List<RecordEntry> records = recordResult.getRecords();
                if (records.size() == 0) {

                    if (result.size() > 0) {
                        //生成文件
                        TextUtils.writeText(result, fileName,filePath);
                        result.clear();

                        logger.info("获取订阅数据至写入文件共耗时>>>>>>>>>>>>>>>>>" + (System.currentTimeMillis() - start) + "毫秒");
                        logger.info("###########################shardId=" + shardId + "###########################");
                        start = System.currentTimeMillis();
                    }

                    // 将最后一次消费点位上报
                    client.commitOffset(offsetCtx);
//                    logger.info("commit offset suc! offset context: " + offsetCtx.toObjectNode().toString());
                    // 可以先休眠一会，再继续消费新记录
//                    thread.sleep(1000);
                    logger.info("sleep 1s and continue consume records! shard id:" + shardId);
                } else {
                    if (!records.isEmpty()) {

                        for (RecordEntry record : records) {
                            // 处理记录逻辑

                            result.add(

                                    record.getBigint("link_id") + common
                                    + record.getString("time_stamp") + common
                                    + record.getBigint("speed") + common
                                    + record.getBigint("offset") + common
                                    + record.getDouble("travel_time") + common
                                    + record.getBigint("reliability_code") + common
                                    + record.getBigint("dir") + common
                                    + record.getBigint("state_code") + common
                                    + record.getBigint("nds_id") + common
                                    + record.getString("ds")
                            );

                            if (result.size() == maxSize) {
                                //生成文件
                                TextUtils.writeText(result, fileName,filePath);

                                result.clear();
                                logger.info("获取订阅数据至写入文件共耗时>>>>>>>>>>>>>>>>>" + (System.currentTimeMillis() - start) + "毫秒");
                                logger.info("###########################shardId=" + shardId + "###########################");
                                start = System.currentTimeMillis();
                            }


                            // 上报点位，该示例是每处理100条记录上报一次点位
                            offsetCtx.setOffset(record.getOffset());
                            recordNum++;
                            if (recordNum % 100 == 0) {
                                client.commitOffset(offsetCtx);
//                                logger.info("commit offset suc! offset context: " + offsetCtx.toObjectNode().toString());
                            }
                        }
                    }
                    cursor = recordResult.getNextCursor();
                }
            } catch (SubscriptionOfflineException e) {
                // 订阅下线，退出
                bExit = true;
                logger.error("订阅下线，退出"+e.getMessage());
                throw new Exception(e);
            } catch (OffsetResetedException e) {
                // 点位被重置，更新offset上下文
                client.updateOffsetContext(offsetCtx);
                cursor = client.getNextOffsetCursor(offsetCtx).getCursor();
//                logger.info("Restart consume shard:" + shardId + ", reset offset:"  + offsetCtx.toObjectNode().toString() + ", cursor:" + cursor);
                logger.error("点位被重置，更新offset上下文"+e.getMessage());
            } catch (OffsetSessionChangedException e) {
                // 其他consumer同时消费了该订阅下的相同shard，退出
                bExit = true;
                logger.error(" 其他consumer同时消费了该订阅下的相同shard，退出"+e.getMessage());
                throw new Exception(e);
            }catch (InvalidCursorException ex) {
                // 非法游标或游标已过期，建议重新定位后开始消费
                // 针对于（the cursor is expired)
                GetCursorResult cursorRs = client.getCursor(projectName, topicName, shardId, CursorType.LATEST);
                cursor = cursorRs.getCursor();

            } catch (Exception e) {
                bExit = true;
                logger.error(" 异常退出 "+e.getMessage());
                throw new Exception(e);
            }
        }

    }
    public void getAmapEvent(Integer maxSize, String shardId) throws Exception {
        long start = System.currentTimeMillis();
        List<String> result = new ArrayList<>();
        //默认130000
        if (maxSize == null || maxSize <= 0) {
            maxSize = 1000;
        }

        boolean bExit = false;
        GetTopicResult topicResult = client.getTopic("city_brain_330100", "ods_amap_event_amap_330100");
        // 首先初始化offset上下文
        OffsetContext offsetCtx = client.initOffsetContext("city_brain_330100", "ods_amap_event_amap_330100", "15275973497528BnKL", shardId);
        String cursor = null; // 开始消费的cursor
        if (!offsetCtx.hasOffset()) {
            // 之前没有存储过点位，先获取初始点位，比如这里获取当前该shard最早的数据
            GetCursorResult cursorResult = client.getCursor("city_brain_330100", "ods_amap_event_amap_330100", shardId, CursorType.OLDEST);
//            GetCursorResult cursorResult = client.getCursor(projectName, topicName, shardId, System.currentTimeMillis() - 24 * 3600 * 1000 /* ms */);

            cursor = cursorResult.getCursor();

        } else {
            // 否则，获取当前已消费点位的下一个cursor

            try{
                cursor = client.getNextOffsetCursor(offsetCtx).getCursor();
            }catch (Exception ex){
                //存在无效的点位  调过无效的点位 跳至当前订阅有效点位最开始的时刻
                if(ex.getMessage().indexOf("Seek Out of Range")>0){
                    GetCursorResult cursorResult = client.getCursor("city_brain_330100", "ods_amap_event_amap_330100", shardId, CursorType.OLDEST);
                    cursor = cursorResult.getCursor();
                }
            }
        }
//        logger.info("Start consume records, begin offset context:" + offsetCtx.toObjectNode().toString()
//                + ", cursor:" + cursor);

        long recordNum = 0L;
        int limit = 1000;
        while (!bExit) {
            try {
                GetRecordsResult recordResult = client.getRecords("city_brain_330100", "ods_amap_event_amap_330100", shardId, cursor, limit,
                        topicResult.getRecordSchema());

                List<RecordEntry> records = recordResult.getRecords();
                if (records.size() == 0) {

                    if (result.size() > 0) {
                        //生成文件
                        TextUtils.writeText(result, "stg_traffic_safety_event_amap",filePath);
                        result.clear();

                        logger.info("获取订阅数据至写入文件共耗时>>>>>>>>>>>>>>>>>" + (System.currentTimeMillis() - start) + "毫秒");
                        logger.info("###########################shardId=" + shardId + "###########################");
                        start = System.currentTimeMillis();
                    }

                    // 将最后一次消费点位上报
                    client.commitOffset(offsetCtx);
//                    logger.info("commit offset suc! offset context: " + offsetCtx.toObjectNode().toString());
                    // 可以先休眠一会，再继续消费新记录
//                    thread.sleep(1000);
                    logger.info("sleep 1s and continue consume records! shard id:" + shardId);
                } else {
                    if (!records.isEmpty()) {

                        for (RecordEntry record : records) {
                            // 处理记录逻辑

                            result.add(

                                    record.getBigint("id") + common
                                            + record.getString("brief") + common
                                            + record.getString("endtime") + common
                                            + record.getString("eventoverview") + common
                                            + record.getString("eventtype") + common
                                            + record.getString("lnglat") + common
                                            + record.getString("nickname") + common
                                            + record.getString("picture") + common
                                            + record.getString("road") + common
                                            + record.getString("shape")+ common
                                            + record.getString("starttime")+ common
                                            + record.getString("updatetime")+ common
                                            + record.getString("recordtime")+ common
                                            + record.getString("ds")+ common
                                            + record.getString("adcode")
                            );

                            if (result.size() == maxSize) {
                                //生成文件
                                TextUtils.writeText(result, "stg_traffic_safety_event_amap",filePath);

                                result.clear();
                                logger.info("获取订阅数据至写入文件共耗时>>>>>>>>>>>>>>>>>" + (System.currentTimeMillis() - start) + "毫秒");
                                logger.info("###########################shardId=" + shardId + "###########################");
                                start = System.currentTimeMillis();
                            }


                            // 上报点位，该示例是每处理100条记录上报一次点位
                            offsetCtx.setOffset(record.getOffset());
                            recordNum++;
                            if (recordNum % 100 == 0) {
                                client.commitOffset(offsetCtx);
//                                logger.info("commit offset suc! offset context: " + offsetCtx.toObjectNode().toString());
                            }
                        }
                    }
                    cursor = recordResult.getNextCursor();
                }
            } catch (SubscriptionOfflineException e) {
                // 订阅下线，退出
                bExit = true;
                logger.error("订阅下线，退出"+e.getMessage());
                throw new Exception(e);
            } catch (OffsetResetedException e) {
                // 点位被重置，更新offset上下文
                client.updateOffsetContext(offsetCtx);
                cursor = client.getNextOffsetCursor(offsetCtx).getCursor();
//                logger.info("Restart consume shard:" + shardId + ", reset offset:"  + offsetCtx.toObjectNode().toString() + ", cursor:" + cursor);
                logger.error("点位被重置，更新offset上下文"+e.getMessage());
            } catch (OffsetSessionChangedException e) {
                // 其他consumer同时消费了该订阅下的相同shard，退出
                bExit = true;
                logger.error(" 其他consumer同时消费了该订阅下的相同shard，退出"+e.getMessage());
                throw new Exception(e);
            }catch (InvalidCursorException ex) {
                // 非法游标或游标已过期，建议重新定位后开始消费
                // 针对于（the cursor is expired)
                GetCursorResult cursorRs = client.getCursor("city_brain_330100", "ods_amap_event_amap_330100", shardId, CursorType.LATEST);
                cursor = cursorRs.getCursor();

            } catch (Exception e) {
                bExit = true;
                logger.error(" 异常退出 "+e.getMessage());
                throw new Exception(e);
            }
        }

    }
    public static void main(String[] args) throws Exception {
        ReadDataHub example = new ReadDataHub();
        try {
            String shardId = "0";
            example.getAmapEvent(100, shardId);
        } catch (DatahubClientException e) {
            e.printStackTrace();
        }
    }
}