package com.iflytek.edmp.demo;

import java.io.IOException;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.io.Writable;

/**
 * Created with Intellij IDEA.
 * User: ztwu2
 * Date: 2018/5/25
 * Time: 13:29
 * Description
 */

public class RecordWriterDemo implements RecordWriter {

    private FSDataOutputStream out;

    public RecordWriterDemo(FSDataOutputStream o) {
        this.out = o;
    }

    public void close(boolean abort) throws IOException {
        out.flush();
        out.close();
    }

    public void write(Writable wr) throws IOException {
        if(wr!=null){
            write(wr.toString());
        }
        write("\n");
    }

    private void write(String str) throws IOException {
        if(StringUtils.isNotBlank(str)){
            JSONObject obj = new JSONObject();
            String[] lines = str.split("#");
            for(String item : lines){
                String[] values = item.split("=");
                obj.put(values[0],values[1]);
            }
            str = obj.toJSONString();
        }
        out.write(str.getBytes(), 0, str.length());
    }

}