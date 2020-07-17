package com.gyb.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.aliyun.datahub.DatahubClient;
import com.aliyun.datahub.DatahubConfiguration;
import com.aliyun.datahub.auth.AliyunAccount;
import com.aliyun.datahub.common.data.Field;
import com.aliyun.datahub.common.data.FieldType;
import com.aliyun.datahub.common.data.RecordSchema;
import com.aliyun.datahub.model.ErrorEntry;
import com.aliyun.datahub.model.ListShardResult;
import com.aliyun.datahub.model.PutRecordsResult;
import com.aliyun.datahub.model.RecordEntry;
import com.gyb.datahub.DatahubReadConfig;
import com.gyb.util.XmlTool;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jms.annotation.JmsListener;

import java.text.SimpleDateFormat;
import java.util.*;
/**
 * 获取MQ消息队列数据写入到datahub
 * @author gyb
 */
public class GetMQMessageController {

    @Autowired
    private DatahubReadConfig datahub;


    @JmsListener(destination = "${spring.activemq.topic-name1}",containerFactory = "topicListener")
    public void readActiveQueue(String message){
        //System.out.println(message);
        try {
            if("" != message){
                JSONObject object = XmlTool.documentToJSONObject(message);
                JSONArray array = (JSONArray) object.get("gpscoord");
                Map<String,Object> result = (Map<String, java.lang.Object>) JSON.parse(array.get(0).toString());
                //System.out.println( result.toString());
                Map<String,String> dataParam = new HashMap<>();
                SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
                String dt = sdf.format(new Date());
                dataParam.put("latitude",result.get("latitude").toString());
                dataParam.put("tonextstopdistance",result.get("tonextstopdistance").toString());
                dataParam.put("velocity",result.get("velocity").toString());
                dataParam.put("dt",dt);
                writeGpsToDatahub(dataParam);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void writeGpsToDatahub(Map<String,String> map){
        AliyunAccount account = new AliyunAccount(datahub.getAccessId(), datahub.getAccessKey());
        DatahubConfiguration conf = new DatahubConfiguration(account, datahub.getEndpoint());
        DatahubClient client = new DatahubClient(conf);
        ListShardResult listShardResult = client.listShard(datahub.getProjectName(), datahub.getTopicName1());
        List<RecordEntry> recordEntries = new ArrayList<RecordEntry>();
        String shardId = listShardResult.getShards().get(0).getShardId();

        RecordSchema schema = new RecordSchema();
        schema.addField(new Field("angle", FieldType.STRING));
        schema.addField(new Field("gprsid", FieldType.STRING));
        schema.addField(new Field("gprssignal", FieldType.STRING));
        schema.addField(new Field("height", FieldType.STRING));
        schema.addField(new Field("busstate", FieldType.STRING));
        schema.addField(new Field("bustemp", FieldType.STRING));
        schema.addField(new Field("latitude", FieldType.STRING));
        schema.addField(new Field("longitude", FieldType.STRING));
        schema.addField(new Field("nextstopno", FieldType.STRING));
        schema.addField(new Field("occurtime", FieldType.STRING));
        schema.addField(new Field("onboardid", FieldType.STRING));
        schema.addField(new Field("runkm", FieldType.STRING));
        schema.addField(new Field("tonextstopdistance", FieldType.STRING));
        schema.addField(new Field("velocity", FieldType.STRING));
        schema.addField(new Field("dt", FieldType.STRING));

        RecordEntry entry = new RecordEntry(schema);
        entry.setString(0,map.get("angle").toString());
        entry.setString(1,map.get("gprsid").toString());
        entry.setString(2,map.get("gprssignal").toString());
        entry.setString(3,map.get("height").toString());
        entry.setString(4,map.get("busstate").toString());
        entry.setString(5,map.get("bustemp").toString());
        entry.setString(6,map.get("latitude").toString());
        entry.setString(7,map.get("longitude").toString());
        entry.setString(8,map.get("nextstopno").toString());
        entry.setString(9,map.get("occurtime").toString());
        entry.setString(10,map.get("onboardid").toString());
        entry.setString(11,map.get("runkm").toString());
        entry.setString(12,map.get("tonextstopdistance").toString());
        entry.setString(13,map.get("velocity").toString());
        entry.setString(14,map.get("dt").toString());

        entry.setShardId(shardId);
        recordEntries.add(entry);
        PutRecordsResult result = client.putRecords(datahub.getProjectName(), datahub.getTopicName1(), recordEntries);
        if (result.getFailedRecordCount() != 0) {
            List<ErrorEntry> errors = result.getFailedRecordError();
        }
    }
}
