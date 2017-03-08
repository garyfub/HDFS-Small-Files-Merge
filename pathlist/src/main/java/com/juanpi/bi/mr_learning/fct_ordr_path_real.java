package com.juanpi.bi.mr_learning;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
/**
 * Created by dianmao on 2017/3/8.
 */
public class fct_ordr_path_real {

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

    public static void cleanoutpath(String outPath) {
        Configuration conf = new Configuration();
        FileSystem fs;
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        try {
            fs = FileSystem.get(new Path(outPath).toUri(), conf);
            // 清理待存放数据的目录
            if (fs.exists(new Path(outPath))) {
                fs.delete(new Path(outPath), true);
            }
        } catch (IOException e) {
            System.out.println(("初始化FileSystem失败！"));
            System.out.println(e.getMessage());
        }
    }

    private static String getInputPath(String dateStr) {
        String parttern = "{0}/date={1}/gu_hash={2}/";
        String path = "hdfs://nameservice1/user/hadoop/dw_realtime/dw_real_path_list_jobs/";
        Pattern guStr =Pattern.compile("[0-9a-f]");
        String inputPath=MessageFormat.format(parttern, path, dateStr, guStr);
        return inputPath;
    }

    private static String getOutputPath(String dateStr) {
        String outputPath = "hdfs://nameservice1/user/dianmao/temp/tmp_path_info/"+dateStr;
        return outputPath;
    }

        public static void main(String[] args) throws Exception {
            fct_ordr_path_real.cleanoutpath(getOutputPath(args[0]));
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "fct_ordr_path_real");
            job.setJarByClass(com.juanpi.bi.mr_learning.fct_ordr_path_real.class);
            job.setMapperClass(com.juanpi.bi.mr_learning.fct_ordr_path_real.TokenizerMapper.class);
            job.setCombinerClass(com.juanpi.bi.mr_learning.fct_ordr_path_real.IntSumReducer.class);
            job.setReducerClass(com.juanpi.bi.mr_learning.fct_ordr_path_real.IntSumReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            FileInputFormat.setInputPaths(job, new Path(getInputPath(args[0])));
            FileOutputFormat.setOutputPath(job, new Path(getOutputPath(args[0])));
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }
    }

