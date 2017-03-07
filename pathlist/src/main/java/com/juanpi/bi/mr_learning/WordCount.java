package com.juanpi.bi.mr_learning;

/**
 * Created by dianmao on 2017/3/6.
 */
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable>{

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }

    private static String getInputPath(String dateStr) {
        String inputPath = "hdfs://nameservice1/user/hive/warehouse/dw.db/fct_page_ref/date=" +dateStr;
        return inputPath;
    }

    private static String getOutputPath(String dateStr) {
        String outputPath = "hdfs://nameservice1/user/dianmao/wordcount/"+dateStr;
        return outputPath;
    }

    public static class IntSumReducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {
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
        job.setMapperClass(com.juanpi.bi.mr_learning.WordCount.TokenizerMapper.class);
        job.setCombinerClass(com.juanpi.bi.mr_learning.WordCount.IntSumReducer.class);
        job.setReducerClass(com.juanpi.bi.mr_learning.WordCount.IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(getInputPath(args[0])));
        FileOutputFormat.setOutputPath(job, new Path(getOutputPath(args[0])));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
