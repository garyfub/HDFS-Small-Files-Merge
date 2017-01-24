package com.juanpi.bi.mr_example;

/**
 * Created by gongzi on 2017/1/24.
 */
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class GroupSort {

    /**
     * map任务
     *
     * */
    public static class GMapper extends Mapper<LongWritable, Text, DescSort, IntWritable>{


        private DescSort tx=new DescSort();
        private IntWritable second=new IntWritable();

        @Override
        protected void map(LongWritable key, Text value,Context context)
                throws IOException, InterruptedException {
            System.out.println("执行map");
            // System.out.println("进map了");
            //mos.write(namedOutput, key, value);
            String ss[]=value.toString().split(";");
            String mkey=ss[0];
            int mvalue=Integer.parseInt(ss[1]);
            tx.setFirstKey(mkey);
            tx.setSecondKey(mvalue);
            second.set(mvalue);
            context.write(tx, second);
        }


    }




    /***
     * Reduce任务
     *
     * **/
    public static class GReduce extends Reducer<DescSort, IntWritable, Text, Text>{
        @Override
        protected void reduce(DescSort arg0, Iterable<IntWritable> arg1, Context ctx)
                throws IOException, InterruptedException {
            System.out.println("执行reduce");
            StringBuffer sb=new StringBuffer();

            for(IntWritable t:arg1){

                // sb.append(t).append(",");


                //con

                ctx.write(new Text(arg0.getFirstKey()), new Text(t.toString()));


                /**这种写法，是这种输出

                 *a  45
                 *b  12
                 b  567
                 三劫   1
                 三劫    32
                 三劫    899
                 秦东亮   34
                 秦东亮   72
                 秦东亮   100
                 */


            }

            if(sb.length()>0){
                sb.deleteCharAt(sb.length()-1);//删除最后一位的逗号
            }


//           在循环里拼接，在循环外输出是这种格式
//           b  12,567
//           三劫 1,32,899
//           秦东亮    34,72,100
            // ctx.write(new Text(arg0.getFirstKey()), new Text(sb.toString()));


        }



    }


    /***
     *
     * 自定义组合键
     * **/
    public static class DescSort implements  WritableComparable{

        public DescSort() {
            // TODO Auto-generated constructor stub
        }
        private String firstKey;
        private int secondKey;


        public String getFirstKey() {
            return firstKey;
        }
        public void setFirstKey(String firstKey) {
            this.firstKey = firstKey;
        }
        public int getSecondKey() {
            return secondKey;
        }
        public void setSecondKey(int secondKey) {
            this.secondKey = secondKey;
        }




        //           @Override
//          public int compare(byte[] arg0, int arg1, int arg2, byte[] arg3,
//                  int arg4, int arg5) {
//              return -super.compare(arg0, arg1, arg2, arg3, arg4, arg5);//注意使用负号来完成降序
//          }
//
//           @Override
//          public int compare(Object a, Object b) {
//
//              return   -super.compare(a, b);//注意使用负号来完成降序
//          }
        @Override
        public void readFields(DataInput in) throws IOException {
            // TODO Auto-generated method stub
            firstKey=in.readUTF();
            secondKey=in.readInt();
        }
        @Override
        public void write(DataOutput out) throws IOException {
            out.writeUTF(firstKey);
            out.writeInt(secondKey);

        }
        @Override
        public int compareTo(Object o) {
            // TODO Auto-generated method stub
            DescSort d=(DescSort)o;
            //this在前代表升序
            return this.getFirstKey().compareTo(d.getFirstKey());
        }


    }


    /**
     * 主要就是对于分组进行排序，分组只按照组建键中的一个值进行分组
     *
     * **/
    public static class TextComparator extends WritableComparator{

        public TextComparator() {
            // TODO Auto-generated constructor stub
            super(DescSort.class,true);//注册Comparator
        }
        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            System.out.println("执行TextComparator分组排序");
            DescSort d1=(DescSort)a;
            DescSort d2=(DescSort)b;

            return  d1.getFirstKey().compareTo(d2.getFirstKey());
        }



    }

    /**
     * 组内排序的策略
     * 按照第二个字段排序
     *
     * */
    public static class TextIntCompartator extends WritableComparator{

        public TextIntCompartator() {
            super(DescSort.class,true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            DescSort d1=(DescSort)a;
            DescSort d2=(DescSort)b;
            System.out.println("执行组内排序TextIntCompartator");
            if(!d1.getFirstKey().equals(d2.getFirstKey())){
                return d1.getFirstKey().compareTo(d2.getFirstKey());
            }else{

                return d1.getSecondKey()-d2.getSecondKey();//0,-1,1

            }
        }

    }

    /**
     * 分区策略
     *
     * */
    public static class KeyPartition extends Partitioner<DescSort, IntWritable>{


        @Override
        public int getPartition(DescSort key, IntWritable arg1, int arg2) {
            // TODO Auto-generated method stub
            System.out.println("执行自定义分区KeyPartition");
            return (key.getFirstKey().hashCode()&Integer.MAX_VALUE)%arg2;
        }
    }


    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        //Configuration conf=new Configuration();
        conf.set("mapred.job.tracker","192.168.75.130:9001");
        //读取person中的数据字段
//        conf.setJar("tt.jar");
        //注意这行代码放在最前面，进行初始化，否则会报


        /**Job任务**/
        Job job=new Job(conf, "testpartion");
        job.setJarByClass(GroupSort.class);
        System.out.println("模式：  "+conf.get("mapred.job.tracker"));
        // job.setCombinerClass(PCombine.class);

        // job.setNumReduceTasks(3);//设置为3
        job.setMapperClass(GMapper.class);
        job.setReducerClass(GReduce.class);

        /**设置分区函数*/
        job.setPartitionerClass(KeyPartition.class);

        //分组函数，Reduce前的一次排序
        job.setGroupingComparatorClass(TextComparator.class);
        //组内排序Map输出完毕后，对key进行的一次排序



        job.setSortComparatorClass(TextIntCompartator.class);

        //TextComparator.class
        //TextIntCompartator.class
        // job.setGroupingComparatorClass(TextIntCompartator.class);
        //组内排序Map输出完毕后，对key进行的一次排序
        // job.setSortComparatorClass(TextComparator.class);

        job.setMapOutputKeyClass(DescSort.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        String path="hdfs://192.168.75.130:9000/root/outputdb";
        FileSystem fs=FileSystem.get(conf);
        Path p=new Path(path);
        if(fs.exists(p)){
            fs.delete(p, true);
            System.out.println("输出路径存在，已删除！");
        }
        FileInputFormat.setInputPaths(job, "hdfs://192.168.75.130:9000/root/input");
        FileOutputFormat.setOutputPath(job,p );
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
