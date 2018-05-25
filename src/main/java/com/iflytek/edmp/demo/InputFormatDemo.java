package com.iflytek.edmp.demo;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

/**
 * Created with Intellij IDEA.
 * User: ztwu2
 * Date: 2018/5/25
 * Time: 13:22
 * Description
 * Hive的InputFormat来源于Hadoop中的对应的部分。
 * 需要注意的是，其采用了mapred的老接口
 */

public class InputFormatDemo extends TextInputFormat implements
        JobConfigurable {

    @Override
    public RecordReader<LongWritable, Text> getRecordReader(InputSplit split,
                                                            JobConf job,
                                                            Reporter reporter) throws IOException {
        reporter.setStatus(split.toString());
        return new RecordReaderDemo(job, (FileSplit) split);
    }
}
