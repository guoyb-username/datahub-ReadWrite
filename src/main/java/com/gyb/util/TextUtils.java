package com.gyb.util;

import org.apache.commons.lang3.StringUtils;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * text文档工具类
 */
public class TextUtils {

    /**
     * 写入txt文件
     *
     * @param result
     * @param fileName
     * @return
     */
    public static synchronized boolean writeText(List<String> result, String fileName,String filePath)  {
        StringBuffer content = new StringBuffer();
        boolean flag = false;
        BufferedWriter out = null;

        try {

            if (result != null && !result.isEmpty() && StringUtils.isNotEmpty(fileName)) {
                fileName += "_" + DateUtils.getCurrentTime_yyyyMMddHHmmssSSS() + ".txt";
//                System.out.println("fileName>>>>>>>>>>>>>"+fileName);
                File pathFile = new File(filePath);
                if (!pathFile.exists()) {
                    pathFile.mkdir();
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
            }

            if (out != null) {
                try {
                    out.flush();
                    out.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }catch(Exception e){
            e.printStackTrace();
        }finally{
            return flag;
        }
    }


    public static void main(String[] args) {

        List<String> list = new ArrayList<>();
        list.add("5905086632599494553|$|20180305081000|$|22|$|86|$|14.2|$|62|$|-1|$|1|$|5130474965830729816|$|20180305");
        list.add("5905086630452038635|$|20180305081000|$|35|$|134|$|13.8|$|62|$|-1|$|1|$|513047496368324620|$|");
        list.add("-1|$|1|$|5130477626562969640|$|20180305");
        list.add("5905085541678405371|$|20180305081000|$|40|$|0|$|0.0|$|0|$|-1|$|1|$|5130477628710453359|$|20180305");

        writeText(list,"cest_","D:\\export\\txt");
        
    }

}
