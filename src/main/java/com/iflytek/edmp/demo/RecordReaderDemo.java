package com.iflytek.edmp.demo;

import java.io.IOException;
import java.util.Iterator;
import java.util.Set;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.LineRecordReader;
import org.apache.hadoop.mapred.RecordReader;

/**
 * Created with Intellij IDEA.
 * User: ztwu2
 * Date: 2018/5/25
 * Time: 13:23
 * Description
 */

public class RecordReaderDemo implements RecordReader<LongWritable, Text> {

    // Reader
    private LineRecordReader reader;
    // The current line_num and lin
    private LongWritable lineKey = null;
    private Text lineValue = null;

    public RecordReaderDemo(JobConf job, FileSplit split) throws IOException {
        reader = new LineRecordReader(job, split);
        lineKey = reader.createKey();
        lineValue = reader.createValue();
    }

    public void close() throws IOException {
        reader.close();
    }

    public boolean next(LongWritable key, Text value) throws IOException {
        while (true) {
            if (!reader.next(lineKey, lineValue)) {
                break;
            }
            //每行记录的key
            key.set(key.get() + 1);

            String data = lineValue.toString();
            if(StringUtils.isNotBlank(data)){
                JSONObject obj = JSONObject.parseObject(data);
                if(obj!=null){
                    Set<String> keys = (Set<String>)obj.keySet();
                    StringBuffer sb = new StringBuffer();
                    Iterator<String> iterator = keys.iterator();
                    while (iterator.hasNext()){
                        String item = iterator.next();
                        sb.append(item+"="+obj.getString(item));
                        sb.append("#");
                    }
                    value.set(sb.deleteCharAt(sb.length() - 1).toString());
                }else {
                    value.set("json error");
                }
            }else {
                value.set("null");
            }
            return true;
        }
        return false;
    }

    public float getProgress() throws IOException {
        return reader.getProgress();
    }

    public LongWritable createKey() {
        return new LongWritable(0);
    }

    public Text createValue() {
        return new Text("");
    }

    public long getPos() throws IOException {
        return reader.getPos();
    }

}
