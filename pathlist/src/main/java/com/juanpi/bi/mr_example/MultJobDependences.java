package com.juanpi.bi.mr_example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 1 先将数据开发,按照大小来进行分区
 * 2 将每个部分的数据进行汇总加和在输出到目录中
 */
public class MultJobDependences {

    class SplitMap extends Mapper<LongWritable,Text,Text,Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.write(value,new Text());
        }
    }
    //第一个job的reduce
    class SplitReduce extends Reducer<Text,Text,Text,Text>{
        @Override
        protected void reduce(Text  key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int k = Integer.parseInt(key.toString());

            if(k <= 5){
                context.write(new Text("<=5"),key);
            }else{
                context.write(new Text(">=5"),key);
            }
        }
    }

    //第二个job的Map
    class SecMap extends Mapper<LongWritable,Text,Text,Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();


            String v = line.substring(line.indexOf("=5") + 2);
            String k = null;

            if(line.contains("<=5")){
                k = "<=5";
            }else{
                k = ">=5";
            }
            context.write(new Text(k),new Text(v.trim().replaceAll("[^0-9]","")));
        }
    }
    //第二个job的reduce
    class SecReduce extends Reducer<Text,Text,Text,IntWritable>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int sum = 0;

            for(Text text : values){
                int v = Integer.parseInt(text.toString().trim());

                sum += v;
            }

            context.write(key,new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws  Exception{
        Configuration conf = new Configuration();

        Job job1 = Job.getInstance(conf,"split");
        job1.setJarByClass(MultJobDependences.class);

        job1.setMapperClass(SplitMap.class);
        job1.setReducerClass(SplitReduce.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        //文件输出
        FileInputFormat.addInputPath(job1,new Path("/user/hive/warehouse/ym.db/test"));
        FileOutputFormat.setOutputPath(job1,new Path("/output/mult-job/tmp"));

        //添加控制job
        ControlledJob cj1 = new ControlledJob(conf);
        cj1.setJob(job1);

        Configuration conf2 = new Configuration();
        //配置第二个job
        Job job2 = Job.getInstance(conf2,"sum");
        job2.setJarByClass(MultJobDependences.class);

        job2.setMapperClass(SecMap.class);
        job2.setReducerClass(SecReduce.class);

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);

        //
        ControlledJob cj2 = new ControlledJob(conf2);
        cj2.setJob(job2);
        cj2.addDependingJob(cj1);

        // job2的输入数据是job1的输出目录
        FileInputFormat.addInputPath(job2,new Path("/output/mult-job/tmp"));
        FileOutputFormat.setOutputPath(job2,new Path("/output/mult-job/mjob"));

        //添加job依赖任务
        JobControl jc = new JobControl("mult-job");
        jc.addJob(cj1);
        jc.addJob(cj2);

        Thread t = new Thread(jc);
        t.start();

        while(true){
            if(jc.allFinished()){
                System.out.println(jc.getSuccessfulJobList());
                jc.stop();
                break;
            }
        }
    }
}