package com.juanpi.bi.mr_example;

/**
 * Created by gongzi on 2016/8/23.
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

public class MultiOutPut {

    public static class MapClass
            extends Mapper<LongWritable, Text, NullWritable, Text> {
        private MultipleOutputs mos;

        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException {
            super.setup(context);
            mos = new MultipleOutputs(context);
        }

        @Override
        protected void map(LongWritable key,
                           Text value,
                           Context context)
                throws IOException, InterruptedException {
            mos.write(NullWritable.get(), value,
                    generateFileName(value));
        }

        private String generateFileName(Text value) {
            String[] split = value.toString().split(",", -1);
            String country = split[4].substring(1, 3);
            return country + "/";
        }

        @Override
        protected void cleanup(Context context)
                throws IOException, InterruptedException {
            super.cleanup(context);
            mos.close();
        }
    }

    public static void main(String[] args)
            throws IOException, ClassNotFoundException,
            InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "MulOutput");
        String[] remainingArgs =
                new GenericOptionsParser(conf, args)
                        .getRemainingArgs();

        if (remainingArgs.length != 2) {
            System.err.println("Error!");
            System.exit(1);
        }
        Path in = new Path(remainingArgs[0]);
        Path out = new Path(remainingArgs[1]);

        FileInputFormat.setInputPaths(job, in);
        FileOutputFormat.setOutputPath(job, out);

        job.setJarByClass(MultiOutPut.class);
        job.setMapperClass(MapClass.class);
        job.setInputFormatClass(TextInputFormat.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(0);
        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

