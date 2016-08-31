package com.juanpi.bi.mapred;

import com.google.common.base.Joiner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import static org.apache.hadoop.io.WritableComparator.readVLong;

/**
 * 烈烈
 * Created by kaenr on 2016/7/13.
 * Updated by gongzi@juanpi.com on 2016-08-12
 * 使用 MultipleOutputs 的原因：数据目录时同时读取，需要根据数据中的gu_id，才能将数据分开
 *
 */
public class PathListNew {

    static String base = "hdfs://nameservice1/user/hadoop/gongzi";

    static final String INPUT_PATH_BASE = "hdfs://nameservice1/user/hadoop/gongzi/dw_real_for_path_list";

    static Configuration conf = new Configuration();

    static FileSystem fs;

    private static void getFileSystem(String basePath) throws IOException {
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        fs = FileSystem.get(new Path(basePath).toUri(), conf);
    }

    /**
     *
     * @param outPath
     */
    public static void cleanDataPath(String outPath)
    {
        // 清空数据输出的目录
        try {
            if(fs.exists(new Path(outPath))){
                fs.delete(new Path(outPath), true);
            }
        } catch (IOException e) {
            System.out.println(("初始化FileSystem失败！"));
            System.out.println(e.getMessage());
        }
    }

    /**
     * 日期格式化
     * 参考 http://bijian1013.iteye.com/blog/2306763
     * @return
     */
    public static String getDateStr()
    {
        Calendar c1 = Calendar.getInstance();
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        return format.format(c1.getTime());
    }

    public static void JobsControl(String dateStr){

        if(dateStr== null || dateStr.isEmpty()){
            dateStr = getDateStr();
        }

        // path_list 数据的目录: /user/hadoop/gongzi/dw_real_path_list/date=2016-08-30/
        String outputPathClean = MessageFormat.format("{0}/{1}/date={2}/", base, "dw_real_path_list", dateStr);

        // 预创建目录
        try {
            getFileSystem(base);
            // 清空即将写 path_list 数据的目录
            cleanDataPath(outputPathClean);

        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("=======>> hadoop FileSystem IOException:" + e.getStackTrace());
        }

        List<String> paths = new ArrayList<>();

        // 遍历16个分区
        for(int i=0x0; i<=0xf; i++) {
            String gu = String.format("%x", i);

            String str = "{0}/{1}/date={2}/gu_hash={3}/";
            String strEvent = MessageFormat.format(str, INPUT_PATH_BASE, "mb_event_hash2", dateStr, gu);
            String strPage = MessageFormat.format(str, INPUT_PATH_BASE, "mb_pageinfo_hash2", dateStr, gu);

            // 文件输入路径
            paths.add(strEvent);
            paths.add(strPage);
        }

        String inputPath = Joiner.on(",").join(paths);

        System.out.println("待处理的数据目录===:" + inputPath);

        try {
            jobConstructor(inputPath, outputPathClean);
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("=======>> hadoop FileSystem IOException:" + e.getStackTrace());
        }
    }

    /**
     *
     * @param inputPath
     * @param outputPath
     * @throws Exception
     */
    public static void jobConstructor(String inputPath, String outputPath) throws Exception {

        Job job = Job.getInstance(conf, "pathListMR_");

        // !! http://stackoverflow.com/questions/21373550/class-not-found-exception-in-mapreduce-wordcount-job
//        job.setJar("pathlist-1.0-SNAPSHOT-jar-with-dependencies.jar");
        job.setJarByClass(PathListNew.class);


        //1.1 指定输入文件路径
        FileInputFormat.setInputPaths(job, inputPath);
        job.setInputFormatClass(TextInputFormat.class);// 指定哪个类用来格式化输入文件

        // -- -- -- -- -- -- -- -- Map -- -- -- -- -- -- -- --

        //1.2指定自定义的Mapper类
        job.setMapperClass(MyMapper.class);

        //指定输出<k2,v2>的类型
        job.setMapOutputKeyClass(NewK2.class);

        job.setMapOutputValueClass(TextArrayWritable.class);

        //1.3 指定分区类
        job.setPartitionerClass(HashPartitioner.class);
        job.setNumReduceTasks(1);

        //1.4 TODO 排序、分区
        job.setGroupingComparatorClass(MyGroupingComparator.class);
        //1.5  TODO （可选）合并

        // -- -- -- -- -- -- -- -- Reduce -- -- -- -- -- -- -- --

        //2.2 指定自定义的reduce类
        job.setReducerClass(MyReducer.class);

        //指定输出<k3,v3>的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //2.3 指定输出到哪里
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        //设定输出文件的格式化类
//        job.setOutputFormatClass(TextOutputFormat.class);
        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);

        //把代码提交给JobTracker执行
        job.waitForCompletion(true);
    }

    static class MyMapper extends Mapper<LongWritable, Text, NewK2, TextArrayWritable> {
        int xx = 0;

        private MultipleOutputs mos;

        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException {
            super.setup(context);
            mos = new MultipleOutputs(context);
        }

        //  throws IOException ,InterruptedException
        @Override
        protected void map(LongWritable key, Text value, Context context){

            final String[] splited = value.toString().split("\001");

            System.out.println("======>>Oooooo: " + splited);
            NewK2 k2 = new NewK2("",0);
            try {
                String gu_id = splited[0];
                String gu = gu_id.substring(gu_id.length() - 1).toLowerCase();

                // gu_id 和starttime 作为联合主键

//                k2 = new NewK2(splited[0], Long.parseLong(splited[22]));
                k2.first = splited[0];
                k2.second = Long.parseLong(splited[22]);

                //page_level_id,page_id,page_value,page_lvl2_value,event_id,event_value,event_lvl2_value,starttime作为 联合value
                // page_level_id  对应的路径 line 每一级别加上 loadtime
                // 21 page_level_id; 15 page_id; 16 page_value; 25: page_lvl2_value; 34: event_id; 35: event_value; 36: event_lvl2_value; 22: starttime
                String loadTime = splited[46];
                String str[] = {splited[21],
                        splited[15]+"\t"+splited[16]+"\t"+splited[25]+"\t"+splited[34]+"\t"+splited[35]+"\t"+splited[36]+"\t"+splited[22] + "\t" + loadTime,
                        value.toString().replace("\001","\t")};
                final TextArrayWritable v2 = new TextArrayWritable(str);

                xx ++;

                mos.write(k2, v2, generateFileName(gu));
            } catch (IOException e)
            {
                e.printStackTrace();
            } catch (InterruptedException e)
            {
                e.printStackTrace();
            } catch(ArrayIndexOutOfBoundsException | NumberFormatException | StringIndexOutOfBoundsException e)
            {
                e.printStackTrace();
                System.out.println("======>>ArrayIndexOutOfBoundsException: " + value.toString());
                System.out.println("======>>ArrayIndexOutOfBoundsException: " + splited);
            } catch (Exception e)
            {
                e.printStackTrace();
                System.out.println("======>>ArrayIndexOutOfBoundsException: " + value.toString());
                System.out.println("======>>ArrayIndexOutOfBoundsException: " + splited);
            }
        }

        // hdfs://nameservice1/user/hadoop/gongzi/dw_real_path_list/date=2016-08-13/gu_hash=0
        // 目录输出格式 date=2016-08-13/gu_hash=0
        private String generateFileName(String gu_hash) {
            return "gu_hash=" + gu_hash + "/";
        }

        @Override
        protected void cleanup(Context context)
                throws IOException, InterruptedException {
            super.cleanup(context);
            mos.close();
        }
    }

    //static class NewValue

    static class MyReducer extends Reducer<NewK2, TextArrayWritable, Text, Text> {

        //1. 定义MultipleOutputs类型变量
        private MultipleOutputs mos;

        @Override
        protected void setup(Reducer.Context context)
                throws IOException, InterruptedException {
            super.setup(context);
            mos = new MultipleOutputs(context);
        }

        protected void reduce(NewK2 k2, Iterable<TextArrayWritable> v2s, Context context) throws IOException ,InterruptedException {
            //long min = Long.MAX_VALUE;
            String initstr = ""+"\t"+""+"\t"+""+"\t"+""+"\t"+""+"\t"+""+"\t"+"";

            String gu_id = k2.first;

            String gu = gu_id.substring(gu_id.length() - 1).toLowerCase();
            Long timeSecond = k2.second;

            for (TextArrayWritable v2 : v2s) {
                String level1 = initstr;
                String level2 = initstr;
                String level3 = initstr;
                String level4 = initstr;
                String level5 = initstr;
                if(Integer.parseInt(v2.toStrings()[0]) == 1){
                    level1=v2.toStrings()[1];
                    level2 = initstr;
                    level3 = initstr;
                    level4 = initstr;
                    level5 = initstr;
                } else if(Integer.parseInt(v2.toStrings()[0]) == 2){
                    level2=v2.toStrings()[1];
                    level3 = initstr;
                    level4 = initstr;
                    level5 = initstr;
                } else if(Integer.parseInt(v2.toStrings()[0]) == 3){
                    level3 = v2.toStrings()[1];
                    level4 = initstr;
                    level5 = initstr;
                } else if(Integer.parseInt(v2.toStrings()[0]) == 4){
                    level4 = v2.toStrings()[1];
                    level5 = initstr;
                } else if(Integer.parseInt(v2.toStrings()[0]) == 5){
                    level5 = v2.toStrings()[1];
                }

                // 4 个级别
                Text key2 = new Text(level1 + "\t" + level2 + "\t" + level3+ "\t" + level4 + "\t" + level5);
                Text value2 = new Text(v2.toStrings()[2]);
                mos.write(key2, value2, generateFileName(gu, timeSecond));
            }
        }

        // hdfs://nameservice1/user/hadoop/gongzi/dw_real_path_list/date=2016-08-13/gu_hash=0
        // 目录输出格式 date=2016-08-13/gu_hash=0
        private String generateFileName(String gu_hash, Long timeSecond) {
//            SimpleDateFormat mDateFormat = new SimpleDateFormat("yyyy-MM-dd");
//            String dateStr = mDateFormat.format(timeSecond);
            return "gu_hash=" + gu_hash + "/";
        }

        @Override
        protected void cleanup(Reducer.Context context)
                throws IOException, InterruptedException {
            super.cleanup(context);
            mos.close();
        }
    }

    /**
     原来的v2不能参与排序，把原来的k2和v2封装到一个类中，作为新的k2
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
    public static void main(String[] args){
        String dateStr = args[0];
        if(dateStr== null || dateStr.isEmpty()){
            JobsControl("");
        } else
        {
            JobsControl(dateStr);
        }
    }
}