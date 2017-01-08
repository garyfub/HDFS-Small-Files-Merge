package com.juanpi.bi.mapred;

import com.google.common.base.Joiner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.List;

import static org.apache.hadoop.io.WritableComparator.readVLong;

/**
 * Created by gongzi on 2016/11/11.
 * 用以更新hive 上dw.fct_path_list 的计算
 * 1、将page_ref_reg 和 event_reg的数据写到一张新的表A
 * 2、通过pathList MAPR读取HDFS上A表数据，计算访问路径，文件落在HDFS路径B
 * 3、B路径下的文件关联到Hive外表C
 * 4、将C表数据insert into 至dw.fct_path_list表中
 */
public class OfflinePathList {
    // hdfs://nameservice1/user/hive/warehouse/dw.db/fct_path_list_mapr
    static String base = "hdfs://nameservice1/user/hive";
    static final String SOURCE_DIR = "fct_path_list_mapr";
    static final String TARGET_DIR = "fct_for_path_list_testoffline";
    static Configuration conf = new Configuration();

    static FileSystem fs;

    public static void getFileSystem(String basePath, String outPath) {

        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");

        try {
            fs = FileSystem.get(new Path(basePath).toUri(), conf);
            // 清理待存放数据的目录
            if(fs.exists(new Path(outPath))){
                fs.delete(new Path(outPath), true);
            }
        } catch (IOException e) {
            System.out.println(("初始化FileSystem失败！"));
            System.out.println(e.getMessage());
        }
    }

    /**
     * eg. hdfs://nameservice1/user/hive/warehouse/dw.db/fct_path_list_mapr/gu_hash=a/
     * @param guStr
     * @return
     */
    private static String getInputPath(String guStr)
    {
        // warehouse/dw.db/fct_path_list_mapr
        String patternStr = "{0}/warehouse/{1}/{2}/gu_hash={3}/";
        String inputPath = MessageFormat.format(patternStr, base, "dw.db", SOURCE_DIR, guStr);
        return inputPath;
    }

    /**
     * eg. hdfs://nameservice1/user/hadoop/dw_realtime/fct_for_path_list_offline/gu_hash=a/
     * @param guStr
     * @return
     */
    private static String getOutputPath(String guStr)
    {
        String patternStr = "{0}/{1}/gu_hash={2}/";
        String outPutPath = MessageFormat.format(patternStr, "hdfs://nameservice1/user/hadoop/dw_realtime", TARGET_DIR, guStr);
        return outPutPath;
    }

    public static void JobsControl(int start, int end, String jobControlName){

        Configuration conf = new Configuration();

        //新建作业控制器
        JobControl jc = new JobControl(jobControlName);

        // 遍历16个分区
        for(int i=start; i<=end; i++) {
            String guStr = String.format("%x", i);

            // 文件输入路径
            String inputPath = getInputPath(guStr);

            // PathList文件落地路径
            String outputPath = getOutputPath(guStr);

            getFileSystem(base, outputPath);

            // 将受控作业添加到控制器中
            // 添加控制job
            try {
                Job job = jobConstructor(inputPath, outputPath, guStr);
                ControlledJob cj = new ControlledJob(conf);
                cj.setJob(job);

                jc.addJob(cj);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        Thread jcThread = new Thread(jc);
        jcThread.start();

        while(true){
            if(jc.allFinished()){
                System.out.println("8个目录的数据处理完毕！");
                System.out.println(jc.getSuccessfulJobList());
                jc.stop();

                // 如果不加 break 或者 return，程序会一直循环
                break;
            }

            if(jc.getFailedJobList().size() > 0){
                List<ControlledJob> failedJobList = jc.getFailedJobList();

                for(ControlledJob fJob : failedJobList)
                {
                    System.out.println("失败的job：" + fJob.getJobName());
                    System.out.println("失败信息：" + fJob.getMessage());
                }
                jc.stop();

                // 如果不加 break 或者 return，程序会一直循环
                break;
            }
        }
    }

    /**
     *
     * @param inputPath
     * @param outputPath
     * @throws Exception
     */
    public static Job jobConstructor(String inputPath, String outputPath, String guStr) throws Exception {

        Job job = Job.getInstance(conf, "OfflinePathList_Partition_" + guStr);

        // !! http://stackoverflow.com/questions/21373550/class-not-found-exception-in-mapreduce-wordcount-job
//        job.setJar("pathlist-1.0-SNAPSHOT-jar-with-dependencies.jar");
        job.setJarByClass(OfflinePathList.class);

        //1.1 指定输入文件路径
        FileInputFormat.setInputPaths(job, inputPath);
        job.setInputFormatClass(TextInputFormat.class);//指定哪个类用来格式化输入文件

        //1.2指定自定义的Mapper类
        job.setMapperClass(OfflinePathList.MyMapper.class);

        //指定输出<k2,v2>的类型
        job.setMapOutputKeyClass(OfflinePathList.NewK2.class);

        job.setMapOutputValueClass(OfflinePathList.TextArrayWritable.class);

        //1.3 指定分区类
        job.setPartitionerClass(HashPartitioner.class);
        job.setNumReduceTasks(1);

        //1.4 TODO 排序、分区
        job.setGroupingComparatorClass(OfflinePathList.MyGroupingComparator.class);
        //1.5  TODO （可选）合并

        //2.2 指定自定义的reduce类
        job.setReducerClass(OfflinePathList.MyReducer.class);

        //指定输出<k3,v3>的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //2.3 指定输出到哪里
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        //设定输出文件的格式化类
        job.setOutputFormatClass(TextOutputFormat.class);

        return job;
    }

    /**
     * 计算层级
     */
    static class MyMapper extends Mapper<LongWritable, Text, OfflinePathList.NewK2, OfflinePathList.TextArrayWritable> {
        int xx = 0;

        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException, ArrayIndexOutOfBoundsException, NumberFormatException {

            final String[] splited = value.toString().split("\001");

            try {
                // gu_id 和starttime 作为联合主键
                String gu_id = splited[1];
                if(!gu_id.isEmpty() && !gu_id.equals("0"))
                {
                    final OfflinePathList.NewK2 k2 = new OfflinePathList.NewK2(splited[1], Long.parseLong(splited[11]));

                    String page_level_id = (splited[0] == null) ? "\\N":splited[0];
                    String page_id = (splited[2] == null) ? "\\N":splited[2];
                    String page_value = (splited[3] == null) ? "\\N":splited[3];
                    String page_lvl2_value = (splited[4] == null) ? "\\N":splited[4];
                    String event_id = (splited[5] == null) ? "\\N":splited[5];
                    String event_value = (splited[6] == null) ? "\\N":splited[6];
                    String event_lvl2_value = (splited[7] == null) ? "\\N":splited[7];
                    String rule_id = (splited[8] == null) ? "\\N":splited[8];
                    String test_id = (splited[9] == null) ? "\\N":splited[9];
                    String select_id = (splited[10] == null) ? "\\N":splited[10];
                    String starttime = (splited[11] == null) ? "\\N":splited[11];
                    String pit_type = (splited[12] == null) ? "\\N":splited[12];
                    String sortdate = (splited[13] == null) ? "\\N":splited[13];
                    String sorthour = (splited[14] == null) ? "\\N":splited[14];
                    String lplid = (splited[15] == null) ? "\\N":splited[15];
                    String ptplid = (splited[16] == null) ? "\\N":splited[16];
                    String ug_id  = (splited[17] == null) ? "\\N":splited[17];

                    // 推荐点击为入口页(购物袋页、品牌页、商祥页底部)
                    String pageLvlId = CommonLogic.getPageLevelId(page_level_id, page_id, event_id);

                    String str[] = {
                            pageLvlId,
                            page_id
                                    + "\t" + page_value
                                    + "\t" + page_lvl2_value
                                    + "\t" + event_id
                                    + "\t" + event_value
                                    + "\t" + event_lvl2_value
                                    + "\t" + starttime
                                    + "\t" + pit_type
                                    + "\t" + sortdate
                                    + "\t" + sorthour
                                    + "\t" + lplid
                                    + "\t" + ptplid
                                    + "\t" + select_id
                                    + "\t" + test_id
                                    + "\t" + ug_id,
                            value.toString().replace("\001", "\t")
                    };

                    final OfflinePathList.TextArrayWritable v2 = new OfflinePathList.TextArrayWritable(str);

                    xx++;

                    context.write(k2, v2);
                }
            } catch (IOException e) {
                e.printStackTrace();
                System.out.println("======>>ArrayIndexOutOfBoundsException: " + value.toString());
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ArrayIndexOutOfBoundsException | NumberFormatException | StringIndexOutOfBoundsException e) {
                e.printStackTrace();
                System.out.println("======>>ArrayIndexOutOfBoundsException: " + Joiner.on("#").join(value.toString().split("\001")));
            } catch (Exception e) {
                e.printStackTrace();
                System.out.println("======>>ArrayIndexOutOfBoundsException: " + value.toString());
            }
        }
    }

    //static class NewValue
    static class MyReducer extends Reducer<OfflinePathList.NewK2, OfflinePathList.TextArrayWritable, Text, Text> {
        protected void reduce(OfflinePathList.NewK2 k2, Iterable<OfflinePathList.TextArrayWritable> v2s, Context context) throws IOException ,InterruptedException {
            String[] initStrArray = {"\\N" ,"\\N" ,"\\N" ,"\\N" ,"\\N" ,"\\N" ,"\\N" ,"\\N","\\N" ,"\\N" ,"\\N" ,"\\N" ,"\\N" ,"\\N" ,"\\N"};
            String initStr = Joiner.on("\t").join(initStrArray);

            String level1 = initStr;
            String level2 = initStr;
            String level3 = initStr;
            String level4 = initStr;
            String level5 = initStr;

            for (OfflinePathList.TextArrayWritable v2 : v2s) {
                try {
                    // 页面层级id
                    String pageLvlIdStr = v2.toStrings()[0];
                    // 页面层级对应的值
                    String pageLvl = v2.toStrings()[1];
                    int pageLvlId = Integer.parseInt(pageLvlIdStr);
                    String path = CommonLogic.getVisitPath(initStr, pageLvlId, pageLvl, level1, level2, level3, level4, level5);
                    // 5 个级别
                    Text key2 = new Text(path);
                    Text value2 = new Text(v2.toStrings()[2]);
                    context.write(key2, value2);
                } catch (Exception e) {
                    e.printStackTrace();
                    System.out.println("======>>Exception: " +  Joiner.on("#").join(v2.toStrings()));
                }
            }
        }
    }

    /**
     原来的v2不能参与排序，把原来的k2和v2封装到一个类中，作为新的k2
     *
     */
    static class  NewK2 implements WritableComparable<OfflinePathList.NewK2> {
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
        public int compareTo(OfflinePathList.NewK2 o) {
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
            if(!(obj instanceof OfflinePathList.NewK2)){
                return false;
            }
            OfflinePathList.NewK2 oK2 = (OfflinePathList.NewK2)obj;
            return (this.first.equals(oK2.first))&&(this.second == oK2.second);
        }
    }

    static class MyGroupingComparator implements RawComparator<OfflinePathList.NewK2> {

        @Override
        public int compare(OfflinePathList.NewK2 o1, OfflinePathList.NewK2 o2) {
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

    /**
     * run this
     */
    private static void run(String dateStr) {
        JobsControl(0x0, 0x8, "OfflinePathList08");
        JobsControl(0x9, 0xf, "OfflinePathList0f");
    }

    /**
     * 分两组并行计算
     * @param args
     */
    public static void main(String[] args){
        run("");

//        {
//            System.out.println(getInputPath("a"));
//            System.out.println(getOutputPath("a"));
//        }
    }
}
