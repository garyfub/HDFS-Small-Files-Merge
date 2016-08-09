package com.juanpi.bi.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import static org.apache.hadoop.io.WritableComparator.readVLong;

/**
 * 烈烈
 * Created by kaenr on 2016/7/13.
 */
public class PathListNew {

//    static final Path INPUT_PATH = new Path("hdfs://nameservice1/user/hadoop/gongzi/dw_real_for_path_list/date=2016-07-30/gu_hash=0/page1470127080000-r-00006");
    static final String INPUT_PATH = "hdfs://nameservice1/user/hadoop/gongzi/dw_real_for_path_list/date=2016-07-30/gu_hash=0/";
    static final String OUT_PATH = "hdfs://nameservice1/user/hadoop/gongzi/dw_real_path_list/date=2016-07-30/gu_hash=0/";

    static Configuration conf = new Configuration();
    static FileSystem fs;

    static {

        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");

        try {
            System.out.println(conf.get("fs.defaultFS"));

            fs = FileSystem.get(new Path(INPUT_PATH).toUri(), conf);
//            listFiles(INPUT_PATH);

            if(fs.exists(new Path(OUT_PATH))){
                fs.delete(new Path(OUT_PATH), true);
            }
        } catch (IOException e) {
            System.out.println(("初始化FileSystem失败！"));
            System.out.println(e.getMessage());
        }
    }

    public static void main(String[] args) throws Exception {
//        System.out.println(PathListNew.class.getClassLoader().getResource("hadoop_conf/core-site.xml"));

//        listFiles(INPUT_PATH);

//        第一个参数传递进来的是hadoop文件系统中的某个文件的URI,以hdfs://ip 的theme开头
//        String uri = args[0];

        final Job job = new Job(conf, PathListNew.class.getSimpleName());

        // http://stackoverflow.com/questions/21373550/class-not-found-exception-in-mapreduce-wordcount-job
        job.setJar("pathlist-1.0-SNAPSHOT-jar-with-dependencies.jar");

        //1.1 指定输入文件路径
        FileInputFormat.setInputPaths(job, INPUT_PATH);
        job.setInputFormatClass(TextInputFormat.class);//指定哪个类用来格式化输入文件

        //1.2指定自定义的Mapper类
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(NewK2.class);//指定输出<k2,v2>的类型
        job.setMapOutputValueClass(TextArrayWritable.class);

        //1.3 指定分区类
        job.setPartitionerClass(HashPartitioner.class);
        job.setNumReduceTasks(1);

        //1.4 TODO 排序、分区
        job.setGroupingComparatorClass(MyGroupingComparator.class);
        //1.5  TODO （可选）合并

        //2.2 指定自定义的reduce类
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);//指定输出<k3,v3>的类型
        job.setOutputValueClass(Text.class);

        //2.3 指定输出到哪里
        FileOutputFormat.setOutputPath(job, new Path(OUT_PATH));
        job.setOutputFormatClass(TextOutputFormat.class);//设定输出文件的格式化类
        job.waitForCompletion(true);//把代码提交给JobTracker执行
    }

//    list all files
    public static void listFiles(String dirName) throws IOException {
        Path f = new Path(dirName);
        FileStatus[] files = fs.listStatus(f);
        System.out.println(dirName + " has all files:");
        for (int i = 0; i< files.length; i++) {
            System.out.println(files[i].getPath().toString());
        }
    }

    static class MyMapper extends Mapper<LongWritable, Text, NewK2, TextArrayWritable> {
        int xx = 0;
        protected void map(LongWritable key, Text value, Context context) throws IOException ,InterruptedException {
            final String[] splited = value.toString().split("\u0001");

            // gu_id和starttime作为联合主键
            // gu_id + starttime
            final NewK2 k2 = new NewK2(splited[0], Long.parseLong(splited[22]));
            //page_level_id,page_id,page_value,page_lvl2_value,event_id,event_value,event_lvl2_value,starttime作为 联合value
            // page_level_id    对应的路径    line
            // 21 page_level_id
            // 9 page_id
            // 10    page_value  14: page_lvl2_value,36: event_id,37: event_value,41: event_lvl2_value, 27: starttime
            String str[] = {splited[21],
                    splited[15]+"\t"+splited[16]+"\t"+splited[25]+"\t"+splited[34]+"\t"+splited[35]+"\t"+splited[36]+"\t"+splited[22],value.toString().replace("\u0001","\t")};
            final TextArrayWritable v2 = new TextArrayWritable(str);

//            System.out.println(xx+  splited[2] +  "日志日志"+  splited[26] + "日志日志"+  splited[13]  + "日志日志"+  splited[9]  );
            xx ++;

            context.write(k2, v2);
        }
    }

    //static class NewValue

    static class MyReducer extends Reducer<NewK2, TextArrayWritable, Text, Text> {
        protected void reduce(NewK2 k2, Iterable<TextArrayWritable> v2s, Context context) throws IOException ,InterruptedException {
            //long min = Long.MAX_VALUE;
            String initstr = ""+"\t"+""+"\t"+""+"\t"+""+"\t"+""+"\t"+""+"\t"+"";

            for (TextArrayWritable v2 : v2s) {
                String level1 = initstr;
                String level2 = initstr;
                String level3 = initstr;
                String level4 = initstr;
                if(Integer.parseInt(v2.toStrings()[0]) == 1){
                    level1=v2.toStrings()[1];
                    level2 = initstr;
                    level3 = initstr;
                    level4 = initstr;
                } else if(Integer.parseInt(v2.toStrings()[0]) == 2){
                    level2=v2.toStrings()[1];
                    level3 = initstr;
                    level4 = initstr;
                } else if(Integer.parseInt(v2.toStrings()[0]) == 3){
                    level3 = v2.toStrings()[1];
                    level4 = initstr;
                } else if(Integer.parseInt(v2.toStrings()[0]) == 4){
                    level4 = v2.toStrings()[1];
                }

/*                Text key2 = new Text(k2.first.toString()+"\t"+k2.second.toString());
                Text value2 = new Text(  level1+"\t"+ level2+"\t"+level3+"\t"+level4+"\t"+v2.toStrings()[2]);*/

                Text key2 = new Text(level1+"\t"+ level2+"\t"+level3+"\t"+level4);
                Text value2 = new Text(v2.toStrings()[2]);
                context.write(key2, value2);
            }

            //context.write(new LongWritable(k2.first), new LongWritable(min));
        };
    }

    /**
     原来的v2不能参与排序，把原来的k2和v2封装到一个类中，作为新的k2
     *
     */
    static class  NewK2 implements WritableComparable<NewK2> {
        String first;
        Long second;

        public NewK2(){}

        public NewK2(String first, long second){
            this.first = first;
            this.second = second;
        }


        @Override
        public void readFields(DataInput in) throws IOException {
            this.first = in.readUTF();
            this.second = in.readLong();
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeUTF(first);
            out.writeLong(second);
        }
        /**
         * 当k2进行排序时，会调用该方法.
         * 当第一列不同时，升序；当第一列相同时，第二列升序
         */
        @Override
        public int compareTo(NewK2 o) {
            final long minus = this.first.compareTo(o.first);
            if(minus !=0){
                return (int)minus;
            }
            return (int)(this.second - o.second);
        }

        @Override
        public int hashCode() {
            return this.first.hashCode()+this.second.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if(!(obj instanceof NewK2)){
                return false;
            }
            NewK2 oK2 = (NewK2)obj;
            return (this.first.equals(oK2.first))&&(this.second==oK2.second);
        }
    }

    static class MyGroupingComparator implements RawComparator<NewK2> {

        @Override
        public int compare(NewK2 o1, NewK2 o2) {
            return (int)(o1.first.compareTo(o2.first));
        }

        @Override
        /*public int compare(byte[] arg0, int arg1, int arg2, byte[] arg3,
                           int arg4, int arg5) {
            return WritableComparator.compareBytes(arg0, arg1, 8, arg3, arg4, 8);
        }*/
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {

            int cmp = 1;
            //determine how many bytes the first VLong takes
            int n1 = WritableUtils.decodeVIntSize(b1[s1]);
            int n2 = WritableUtils.decodeVIntSize(b2[s2]);

            try {
                //read value from VLongWritable byte array
                long l11 = readVLong(b1, s1);
                long l21 = readVLong(b2, s2);

                cmp = l11 > l21 ? 1 : (l11 == l21 ? 0 : -1);
                if (cmp != 0) {

                    return cmp;

                } else {

                    long l12 = readVLong(b1, s1 + n1);
                    long l22 = readVLong(b2, s2 + n2);
                    return cmp = l12 > l22 ? 1 : (l12 == l22 ? 0 : -1);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

    }


    /*新增IntArrayWritable的writable类*/
    static class IntArrayWritable extends ArrayWritable {

        private Long[] values = null;

        public IntArrayWritable() {
            super(LongWritable.class);
        }

        public IntArrayWritable(String[] strings) {
            super(LongWritable.class);
            values = new Long[strings.length];
            for (int i = 0; i < strings.length; i++) {
                values[i] = Long.parseLong(strings[i]);
            }

        }

        @Override
        public String[] toStrings() {
            String[] strings = new String[values.length];
            for (int i = 0; i < values.length; i++) {
                strings[i] = values[i] + "";
            }
            return strings;
        }


        @Override
        public void readFields(DataInput in) throws IOException {
            values = new Long[2];
            for (int i = 0; i < values.length; i++) {
                values[i] = in.readLong();                          // store it in values
            }
        }

        @Override
        public void write(DataOutput out) throws IOException {
            for (int i = 0; i < values.length; i++) {
                out.writeLong(values[i]);
            }
        }
    }


    public static class TextArrayWritable extends ArrayWritable {
        public TextArrayWritable() {
            super(Text.class);
        }

        public TextArrayWritable(String[] strings) {
            super(Text.class);
            Text[] texts = new Text[strings.length];
            for (int i = 0; i < strings.length; i++) {
                texts[i] = new Text(strings[i]);
            }
            set(texts);
        }
    }
}