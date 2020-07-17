package com.gyb.demo;

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

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * 测试写入datahub
 */
public class TestWriteDatahub {
    //,,t1,t2
    public static void main(String args[]){
        //testWriteDatahub();
    }

    public static void testWriteDatahub(){
        String AccessId ="";//id
        String AccessKey = " ";//key
        String Endpoint = "http:// .cn.d01.dh.cloud.cn";//point
        String ProjectName = " ";//project_name
        String TopicName = " ";
        Map<String,String> dataParam = new HashMap<>();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        String dt = sdf.format(new Date());
        dataParam.put("t1","测试写入数据");
        dataParam.put("t2",dt);
        writeToDatahub(dataParam,AccessId,AccessKey,Endpoint,ProjectName,TopicName);
    }

    public static void writeToDatahub(Map<String,String> map, String AccessId, String AccessKey, String Endpoint, String ProjectName, String TopicName){
        AliyunAccount account = new AliyunAccount(AccessId, AccessKey);
        DatahubConfiguration conf = new DatahubConfiguration(account, Endpoint);
        DatahubClient client = new DatahubClient(conf);
        ListShardResult listShardResult = client.listShard(ProjectName, TopicName);
        List<RecordEntry> recordEntries = new ArrayList<RecordEntry>();
        String shardId = listShardResult.getShards().get(0).getShardId();

        RecordSchema schema = new RecordSchema();
        schema.addField(new Field("t1", FieldType.STRING));
        schema.addField(new Field("t2", FieldType.STRING));

        RecordEntry entry = new RecordEntry(schema);
        entry.setString(0,map.get("t1").toString());
        entry.setString(1,map.get("t2").toString());
        entry.setShardId(shardId);
        recordEntries.add(entry);
        PutRecordsResult result = client.putRecords(ProjectName, TopicName, recordEntries);
        if (result.getFailedRecordCount() != 0) {
            List<ErrorEntry> errors = result.getFailedRecordError();
        }
    }

}
