package com.juanpi.bi.mr_learning;

/**
 * Created by dianmao on 2017/3/6.
 */

import java.io.IOException;
import java.text.MessageFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

    private static String getInputPath(String dateStr) {
        String inputPath = "hdfs://nameservice1/user/hive/warehouse/dw.db/fct_page_ref/date=" +dateStr;
        return inputPath;
    }

    private static String getOutputPath(String dateStr) {
        String outputPath = "hdfs://nameservice1/user/dianmao/wordcount/";
        return outputPath;
    }

    public static class MapTest extends Mapper<LongWritable, Text, String, IntWritable>{

        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {
            IntWritable one = new IntWritable(1);
            final String[] splited = value.toString().split("\001");
            final String key1 = splited[0];
                context.write(key1, one);
        }
    }

    public static class ReduceTest extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(com.juanpi.bi.mr_learning.WordCount.class);
        job.setMapperClass(com.juanpi.bi.mr_learning.WordCount.MapTest.class);
        job.setCombinerClass(com.juanpi.bi.mr_learning.WordCount.ReduceTest.class);
        job.setReducerClass(com.juanpi.bi.mr_learning.WordCount.ReduceTest.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(getInputPath(args[0])));
        FileOutputFormat.setOutputPath(job, new Path(getInputPath(args[0])));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
