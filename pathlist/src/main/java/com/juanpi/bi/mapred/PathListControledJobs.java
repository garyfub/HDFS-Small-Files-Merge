package com.juanpi.bi.mapred;

import com.google.common.base.Joiner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;

/**
 * 烈烈
 * Created by kaenr && gongzi on 2016/7/13.
 */
public class PathListControledJobs {

    // TODO 配置文件可以通过zk管理
    static String base = "hdfs://nameservice1/user/hadoop/dw_realtime";

    static final String INPUT_PATH_BASE =
            "hdfs://nameservice1/user/hadoop/dw_realtime/dw_real_for_path_list";

    static final String PATH_JOBS = "dw_real_path_list_jobs";

    static Configuration conf = new Configuration();

    static FileSystem fs;

    public static void getFileSystem(String basePath, String outPath) {

        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");

        try {
            fs = FileSystem.get(new Path(basePath).toUri(), conf);
            // 清理待存放数据的目录
            if (fs.exists(new Path(outPath))) {
                fs.delete(new Path(outPath), true);
            }
        } catch (IOException e) {
            System.out.println(("初始化FileSystem失败！"));
            System.out.println(e.getMessage());
        }
    }

    /**
     *
     * @param dateStr
     * @param guStr
     * @return
     */
    private static String getInputPath(String dateStr, String guStr) {
        String str = "{0}/{1}/date={2}/gu_hash={3}/merged/";
//        for_pathList.topicIds=mb_event_hash2,mb_pageinfo_hash2,pc_events_hash3,jp_hash3
        String strEvent =
                MessageFormat.format(str, INPUT_PATH_BASE, "mb_event_hash2", dateStr, guStr);
        String strPage =
                MessageFormat.format(str, INPUT_PATH_BASE, "mb_pageinfo_hash2", dateStr, guStr);
        String strh5Event =
                MessageFormat.format(str, INPUT_PATH_BASE, "pc_events_hash3", dateStr, guStr);
        String strh5Page=
                MessageFormat.format(str, INPUT_PATH_BASE, "jp_hash3", dateStr, guStr);
        // 文件输入路径
        String inputPath = strEvent + "," + strPage + "," + strh5Event+","+strh5Page;
        return inputPath;
    }

    /**
     * eg. hdfs://nameservice1/user/hadoop/dw_realtime/dw_real_path_list_jobs_test/gu_hash=a/
     */
    private static String getOutputPath(String dateStr, String guStr) {
        // PathList文件落地路径
        String patternStr = "{0}/{1}/date={2}/gu_hash={3}/";
        String outPutPath = MessageFormat.format(patternStr, base, PATH_JOBS, dateStr, guStr);
        return outPutPath;
    }

    /**
     * 参考 http://bijian1013.iteye.com/blog/2306763
     */
    public static String getDateStr() {
        Calendar c1 = Calendar.getInstance();
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        return format.format(c1.getTime());
    }

    public static void JobsControl(String dateStr, int start, int end, String jobControlName) {

        if (dateStr == null || dateStr.isEmpty()) {
            dateStr = getDateStr();
        }

        Configuration conf = new Configuration();

        //新建作业控制器
        JobControl jc = new JobControl(jobControlName);

        // 遍历16个分区
        for (int i = start; i <= end; i++) {
            String guStr = String.format("%x", i);

            String inputPath = getInputPath(dateStr, guStr);

            // PathList文件落地路径
            String outputPath = getOutputPath(dateStr, guStr);

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

        while (true) {
            if (jc.allFinished()) {
                System.out.println("4个目录的数据处理完毕！");
                System.out.println(jc.getSuccessfulJobList());
                jc.stop();
                // 如果不加 break 或者 return，程序会一直循环
                break;
            }

            if (jc.getFailedJobList().size() > 0) {
                System.out.println(jc.getFailedJobList());
                jc.stop();
                // 如果不加 break 或者 return，程序会一直循环
                break;
            }
        }
    }

    static class MyMapper extends
            Mapper<LongWritable, Text, PathListControledJobs.NewK2, PathListControledJobs.TextArrayWritable> {
        int xx = 0;

        protected void map(LongWritable key, Text value, Context context) {

            final String[] splited = value.toString().split("\001");

            try {
                // gu_id 和starttime 作为联合主键
                String gu_id = splited[0];
                if (!gu_id.isEmpty() && !gu_id.equals("0")) {
                    String timeStr = (splited[22] == null) ? "\\N" : splited[22];
                    long startTime = Long.parseLong(timeStr);

                    final PathListControledJobs.NewK2 k2 =
                            new PathListControledJobs.NewK2(gu_id, startTime);

                    String pageLevelId = (splited[21] == null) ? "\\N" : splited[21];
                    String pageId = (splited[15] == null) ? "\\N" : splited[15];
                    String page_value = (splited[16] == null) ? "\\N" : splited[16];
                    String page_lvl2_value = (splited[25] == null) ? "\\N" : splited[25];
                    String eventId = (splited[40] == null) ? "\\N" : splited[40];
                    String event_value = (splited[41] == null) ? "\\N" : splited[41];
                    String event_lvl2_value = (splited[42] == null) ? "\\N" : splited[42];

                    String loadTime = (splited[46] == null) ? "\\N" : splited[46];

                    String testId = (splited[44] == null) ? "\\N" : splited[44];
                    String selectId = (splited[45] == null) ? "\\N" : splited[45];
                    String pitType = (splited[27] == null) ? "\\N" : splited[27];
                    String sortDate = (splited[28] == null) ? "\\N" : splited[28];
                    String sortHour = (splited[29] == null) ? "\\N" : splited[29];
                    String lplid = (splited[30] == null) ? "\\N" : splited[30];
                    String ptplid = (splited[31] == null) ? "\\N" : splited[31];
                    String ug_id = (splited[47] == null) ? "\\N" : splited[47];

                    // 推荐点击为入口页(购物袋页、品牌页、商祥页底部)
                    String pageLvlId = pageLevelId;

                    String str[] = {
                            pageLvlId,
                            pageId
                                    + "\t" + page_value
                                    + "\t" + page_lvl2_value
                                    + "\t" + eventId
                                    + "\t" + event_value
                                    + "\t" + event_lvl2_value
                                    + "\t" + timeStr
                                    + "\t" + loadTime
                                    + "\t" + testId
                                    + "\t" + selectId
                                    + "\t" + pitType
                                    + "\t" + sortDate
                                    + "\t" + sortHour
                                    + "\t" + lplid
                                    + "\t" + ptplid
                                    + "\t" + ug_id,
                            value.toString().replace("\001", "\t")
                    };

                    final PathListControledJobs.TextArrayWritable v2 =
                            new PathListControledJobs.TextArrayWritable(str);

                    xx++;

                    context.write(k2, v2);
                } else {
                    System.out.println("======>>mapper gu_id is invalid: " + value.toString());
                }
            } catch (IOException e) {
                e.printStackTrace();
                System.out.println("======>>IOException: " + value.toString());
            } catch (InterruptedException e) {
                e.printStackTrace();
                System.out.println("======>>InterruptedException: " + value.toString());
            } catch (ArrayIndexOutOfBoundsException e) {
                e.printStackTrace();
                System.out.println("======>>ArrayIndexOutOfBoundsException: " + value.toString());
            } catch (StringIndexOutOfBoundsException e) {
                e.printStackTrace();
                System.out.println("======>>StringIndexOutOfBoundsException: " + value.toString());
            } catch (NumberFormatException e) {
                e.printStackTrace();
                System.out.println("======>>NumberFormatException: " + value.toString());
            } catch (Exception e) {
                e.printStackTrace();
                System.out.println("======>>Exception: " + value.toString());
            }
        }
    }

    static class MyReducer extends
            Reducer<PathListControledJobs.NewK2, PathListControledJobs.TextArrayWritable, Text, Text> {

        protected void reduce(PathListControledJobs.NewK2 k2,
                              Iterable<PathListControledJobs.TextArrayWritable> valueArray, Context context)
                throws IOException, InterruptedException {
            String[] initStrArray =
                    {"\\N", "\\N", "\\N", "\\N", "\\N", "\\N", "\\N", "\\N", "\\N", "\\N", "\\N", "\\N", "\\N", "\\N", "\\N", "\\N"};
            String initStr = Joiner.on("\t").join(initStrArray);

            String level1 = initStr;
            String level2 = initStr;
            String level3 = initStr;
            String level4 = initStr;
            String level5 = initStr;

            String guId = k2.first;

            for (PathListControledJobs.TextArrayWritable va : valueArray) {

                try {
                    // 0: page_level_id, 1: 层级, 2 最新的那条记录
                    String pageLvlIdStr = va.toStrings()[0];
                    String pageLvl = va.toStrings()[1];

                    int pageLvlId = Integer.parseInt(pageLvlIdStr);

                    if (pageLvlId == 1) {
                        level1 = pageLvl;
                        level2 = initStr;
                        level3 = initStr;
                        level4 = initStr;
                        level5 = initStr;
                    } else if (pageLvlId == 2) {
                        level2 = pageLvl;
                        level3 = initStr;
                        level4 = initStr;
                        level5 = initStr;
                    } else if (pageLvlId == 3) {
                        level3 = pageLvl;
                        level4 = initStr;
                        level5 = initStr;
                    } else if (pageLvlId == 4) {
                        level4 = pageLvl;
                        level5 = initStr;
                    } else if (pageLvlId == 5) {
                        level5 = pageLvl;
                    }

                    String keyStr =
                            level1 + "\t" + level2 + "\t" + level3 + "\t" + level4 + "\t" + level5;

                    // 5 个级别
                    Text key2 = new Text(keyStr);
                    Text value2 = new Text(va.toStrings()[2]);
                    context.write(key2, value2);
                } catch (Exception e) {
                    e.printStackTrace();
                    System.out.println(
                            "======>>Reduce Exception: " + Joiner.on("#").join(va.toStrings()));
                }
            }
        }
    }

    /**
     * 分区函数类。根据first确定Partition。
     */
    public static class FirstPartitioner extends Partitioner<NewK2, Text> {
        @Override
        public int getPartition(PathListControledJobs.NewK2 key, Text value, int numPartitions) {
            return Math.abs(key.first.hashCode() * 127) % numPartitions;
        }
    }

    /**
     * 原来的v2不能参与排序，把原来的k2和v2封装到一个类中，作为新的k2
     */
    static class NewK2 implements WritableComparable<PathListControledJobs.NewK2> {
        private String first;
        private Long second;

        public NewK2() {
        }

        public NewK2(String first, long second) {
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
         * 当 NewK2 进行排序时，会调用该方法.
         * 当第一列不同时，升序；当第一列相同时，第二列升序
         */
        @Override
        public int compareTo(PathListControledJobs.NewK2 o) {
            // this在前, 代表升序
            final int minus = this.first.compareTo(o.first);
            if (minus != 0) {
                return minus;
            }
            return (int) (this.second - o.second);
        }

        @Override
        public int hashCode() {
            return this.first.hashCode() + this.second.hashCode();
        }

        @Override
        public boolean equals(Object obj) {

            if (!(obj instanceof PathListControledJobs.NewK2)) {
                return false;
            }

            PathListControledJobs.NewK2 oK2 = (PathListControledJobs.NewK2) obj;
            return (this.first.equals(oK2.first)) && (this.second == oK2.second);
        }
    }

    /**
     * 根据gu_id来分组
     */
    public static class GroupingComparator extends WritableComparator {
        /**
         * key1,key2,buffer是记录hashmap对应的key值，用于WritableComparator的构造函数，
         * 但由其构造函数中我们可以看出WritableComparator根据creaeteInstance来判断是否实例化key1，key2，和buffer
         */
        protected GroupingComparator() {
            super(NewK2.class, true);
        }

        //Compare two WritableComparables.
        //  重载 compare：对组合键按第一个自然键排序分组
        public int compare(WritableComparable w1, WritableComparable w2) {
            NewK2 ip1 = (NewK2) w1;
            NewK2 ip2 = (NewK2) w2;
            // 根据gu_id来分组
            String left = ip1.first;
            String right = ip2.first;
            /**
             * len1 - len2
             * 0 相等
             * < 0 left < right
             * > 0 left > right
             */
            return left.compareTo(right);
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
     * @throws Exception
     */
    public static Job jobConstructor(String inputPath, String outputPath, String guHash)
            throws Exception {

        Job job = Job.getInstance(conf, "PathListReal_" + guHash);

        // !! http://stackoverflow.com/questions/21373550/class-not-found-exception-in-mapreduce-wordcount-job
        //        job.setJar("pathlist-1.0-SNAPSHOT-jar-with-dependencies.jar");
        job.setJarByClass(PathListControledJobs.class);
        // 1.2指定自定义的 Mapper 类
        job.setMapperClass(PathListControledJobs.MyMapper.class);
        //2.2 指定自定义的reduce类
        job.setReducerClass(PathListControledJobs.MyReducer.class);

        // group and partition by the first int in the pair
        job.setPartitionerClass(FirstPartitioner.class);
        job.setGroupingComparatorClass(PathListControledJobs.GroupingComparator.class);

        // the map output is NewK2, TextArrayWritable
        job.setMapOutputKeyClass(PathListControledJobs.NewK2.class);
        job.setMapOutputValueClass(PathListControledJobs.TextArrayWritable.class);

        // the reduce output is Text, Text
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // 指定输入文件路径
        FileInputFormat.setInputPaths(job, inputPath);
        // 指定输出到哪里
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        return job;
    }

    /**
     * 分两组并行计算
     */
    public static void main(String[] args) {
        String dateStr = args[0];
        System.out.println("===========>> PathListControledJobs start 2017-02-23 !<<===========");
        if (dateStr == null || dateStr.isEmpty()) {
            JobsControl("", 0x0, 0x3, "PathListControledJobs01");
            JobsControl("", 0x4, 0x7, "PathListControledJobs04");
            JobsControl("", 0x8, 0xb, "PathListControledJobs08");
            JobsControl("", 0xc, 0xf, "PathListControledJobs0c");
        } else {
            JobsControl(dateStr, 0x0, 0x3, "PathListControledJobs01");
            JobsControl(dateStr, 0x4, 0x7, "PathListControledJobs04");
            JobsControl(dateStr, 0x8, 0xb, "PathListControledJobs08");
            JobsControl(dateStr, 0xc, 0xf, "PathListControledJobs0c");
        }
    }
}