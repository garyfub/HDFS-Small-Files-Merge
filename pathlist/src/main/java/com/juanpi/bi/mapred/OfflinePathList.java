package com.juanpi.bi.mapred;

import com.google.common.base.Joiner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.hive.ql.io.orc.OrcNewOutputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.orc.TypeDescription;
import org.apache.orc.mapred.OrcList;
import org.apache.orc.mapred.OrcStruct;

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
//    static final String TARGET_DIR = "fct_for_path_list_offline";
    static final String TARGET_DIR = "test";
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
        job.setJarByClass(OfflinePathList.class);

        // 指定输入文件路径
        FileInputFormat.setInputPaths(job, inputPath);
        job.setInputFormatClass(TextInputFormat.class);//指定哪个类用来格式化输入文件

        // 指定自定义的Mapper类
        job.setMapperClass(MyMapper.class);

        // 指定输出<k2,v2>的类型
        job.setMapOutputKeyClass(NewK2.class);

        job.setMapOutputValueClass(TextArrayWritable.class);

        // 指定分区类
        job.setPartitionerClass(HashPartitioner.class);

        job.setNumReduceTasks(1);

        // TODO 排序、分区
        job.setGroupingComparatorClass(MyGroupingComparator.class);

        //2.2 指定自定义的reduce类
        job.setReducerClass(MyReducer.class);

        //设置最终输出结果<key,value>类型；
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Writable.class);

        //2.3 指定输出到哪里
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        //设定输出文件的格式化类
        // job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputFormatClass(OrcNewOutputFormat.class);


        return job;
    }

    /**
     * 计算层级
     */
    static class MyMapper extends Mapper<LongWritable, Text, NewK2, TextArrayWritable> {
        int xx = 0;

        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException, ArrayIndexOutOfBoundsException, NumberFormatException {

            final String[] splited = value.toString().split("\001");

            try {
                // gu_id 和 starttime_origin 作为联合主键
                String gu_id = splited[1];
                if(!gu_id.isEmpty() && !gu_id.equals("0"))
                {
                    final NewK2 k2 = new NewK2(splited[1], Long.parseLong(splited[11]));

                    String pageLevelId = (splited[0] == null) ? "0":splited[0];
                    String pageId = (splited[2] == null) ? "0":splited[2];
                    String page_value = (splited[3] == null) ? "0":splited[3];
                    String page_lvl2_value = (splited[4] == null) ? "0":splited[4];
                    String eventId = (splited[5] == null) ? "0":splited[5];
                    String event_value = (splited[6] == null) ? "0":splited[6];
                    String event_lvl2_value = (splited[7] == null) ? "0":splited[7];
                    String test_id = (splited[9] == null) ? "0":splited[9];
                    String select_id = (splited[10] == null) ? "0":splited[10];
                    String starttime = (splited[11] == null) ? "0":splited[11];
                    String pit_type = (splited[12] == null) ? "0":splited[12];
                    String sortdate = (splited[13] == null) ? "0":splited[13];
                    String sorthour = (splited[14] == null) ? "0":splited[14];
                    String lplid = (splited[15] == null) ? "0":splited[15];
                    String ptplid = (splited[16] == null) ? "0":splited[16];
                    String ug_id  = (splited[17] == null) ? "0":splited[17];

                    // 推荐点击为入口页(购物袋页、品牌页、商祥页底部)
                    String pageLvlId = pageLevelId;

                    // 推荐点击为入口页(购物袋页、品牌页、商祥页底部)
                    if("481".equals(eventId) || "10041".equals(eventId)){
                        if("158".equals(pageId) || "167".equals(pageId) || "250".equals(pageId) || "26".equals(pageId)) {
                            pageLvlId = "1";
                        }
                    } else if("10043".equals(eventId)){
                        if("10084".equals(pageId) || "10085".equals(pageId)){
                            pageLvlId = "5";
                        }
                    } else if("10050".equals(eventId)){
                        if("10085".equals(pageId) || "10107".equals(pageId)){
                            pageLvlId = "5";
                        }
                    } else if("448".equals(eventId)){
                        if("158".equals(pageId)){
                            pageLvlId = "5";
                        }
                    }

                    String str[] = {
                            pageLvlId,
                            pageId
                                    + "\t" + page_value
                                    + "\t" + page_lvl2_value
                                    + "\t" + eventId
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

                    final TextArrayWritable v2 = new TextArrayWritable(str);

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

    public static class MyORCReducer
            extends Reducer<Text,IntWritable,NullWritable,OrcStruct> {

        private TypeDescription schema =
                TypeDescription.fromString("struct<key:string,ints:array<int>>");

        // createValue creates the correct value type for the schema
        private OrcStruct pair = (OrcStruct) OrcStruct.createValue(schema);

        // get a handle to the list of ints
        private OrcList<IntWritable> valueList =
                (OrcList<IntWritable>) pair.getFieldValue(1);

        private final NullWritable nada = NullWritable.get();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context output) throws IOException, InterruptedException
        {
            pair.setFieldValue(0, key);
            valueList.clear();
            for(IntWritable val: values) {
                valueList.add(new IntWritable(val.get()));
            }
            output.write(nada, pair);
        }
    }

    public static class ORCReducer extends
            Reducer<NewK2, TextArrayWritable, NullWritable, OrcStruct> {
        private TypeDescription schema = TypeDescription.fromString("struct<key:string,value:string>");

        private OrcStruct pair = (OrcStruct) OrcStruct.createValue(schema);

        private final NullWritable nw = NullWritable.get();

        public void reduce(Text key, Iterable<Text> values, Context output)
                throws IOException, InterruptedException {
            for (Text val : values) {
                pair.setFieldValue(0, key);
                pair.setFieldValue(1, val);
                output.write(nw, pair);
            }
        }
    }

    // static class NewValue
    static class MyReducer extends Reducer<NewK2, TextArrayWritable, NullWritable, Writable>
    {

//        private static final String SCHEMA = "<gu_id:string, endtime:bigint, last_entrance_page_id:int, last_guide_page_id:int, last_before_goods_page_id:int, last_entrance_page_value:string, last_guide_page_value:string, last_before_goods_page_value:string, last_entrance_event_id:int, last_guide_event_id:int, last_before_goods_event_id:int, last_entrance_event_value:string, last_guide_event_value:string, last_before_goods_event_value:string, last_entrance_timestamp:bigint, last_guide_timestamp:bigint, last_before_goods_timestamp:bigint, guide_lvl2_page_id:int, guide_lvl2_page_value:string, guide_lvl2_event_id:int, guide_lvl2_event_value:string, guide_lvl2_timestamp:bigint, guide_is_del:int, guide_lvl2_is_del:int, before_goods_is_del:int, entrance_page_lvl2_value:string, guide_page_lvl2_value:string, guide_lvl2_page_lvl2_value:string, before_goods_page_lvl2_value:string, entrance_event_lvl2_value:string, guide_event_lvl2_value:string, guide_lvl2_event_lvl2_value:string, before_goods_event_lvl2_value:string, rule_id:string, test_id:string, select_id:string, last_entrance_pit_type:int, last_entrance_sortdate:string, last_entrance_sorthour:int, last_entrance_lplid:int, last_entrance_ptplid:int, last_entrance_ug_id:int>";
//
//        private final TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(SCHEMA);
//
//        private final OrcSerde serde = new OrcSerde();
//
//        private final ObjectInspector inspector = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(typeInfo);
//
//        private Writable row;

        OrcSerde serde = new OrcSerde();
        Writable row;
        StructObjectInspector inspector = (StructObjectInspector) ObjectInspectorFactory
                .getReflectionObjectInspector(Row.class,
                        ObjectInspectorFactory.ObjectInspectorOptions.JAVA);

        protected void reduce(NewK2 k2,
                              Iterable<TextArrayWritable> v2s,
                              Context context) throws IOException,
                InterruptedException
        {
            String[] initStrArray = {"0" ,"0" ,"0" ,"0" ,"0" ,"0" ,"0" ,"0","0" ,"0" ,"0" ,"0" ,"0" ,"0" ,"0"};
            String initStr = Joiner.on("\t").join(initStrArray);

            String level1 = initStr;
            String level2 = initStr;
            String level3 = initStr;
            String level4 = initStr;
            String level5 = initStr;

            for (TextArrayWritable v2 : v2s) {

                try {
                    String pageLvlIdStr = v2.toStrings()[0];
                    String pageLvl = v2.toStrings()[1];
                    int pageLvlId = Integer.parseInt(pageLvlIdStr);

                    if(pageLvlId == 1 || pageLvlId == 2){
                        level1= pageLvl;
                        level2 = initStr;
                        level3 = initStr;
                        level4 = initStr;
                        level5 = initStr;
                    } else if(pageLvlId == 3){
                        level3 = pageLvl;
                        level4 = initStr;
                        level5 = initStr;
                    } else if(pageLvlId == 4){
                        level4 = pageLvl;
                        level5 = initStr;
                    } else if(pageLvlId == 5){
                        level5 = pageLvl;
                    }

                    String keyStr = level1 + "\t" + level2 + "\t" + level3+ "\t" + level4 + "\t" + level5;

                    // 5 个级别
                    Text key2 = new Text(keyStr);
                     Text value2 = new Text(v2.toStrings()[2]);
                    String[] result = {keyStr, v2.toStrings()[2]};
                    row = serde.serialize(new Row(result), inspector);
//                    row = serde.serialize(v2.toStrings()[2], inspector);
                    context.write(NullWritable.get(), row);
                } catch (Exception e) {
                    e.printStackTrace();
                    System.out.println("======>>Exception: " +  Joiner.on("#").join(v2.toStrings()));
                }
            }
        }
    }

    static class Row implements Writable {
        String key;
        String value;

        Row(String[] val){
            this.key = val[0];
            this.value = val[1];
        }
        @Override
        public void readFields(DataInput arg0) throws IOException {
            throw new UnsupportedOperationException("no write");
        }
        @Override
        public void write(DataOutput arg0) throws IOException {
            throw new UnsupportedOperationException("no read");
        }
    }

    /**
     * 原来的v2不能参与排序，把原来的k2和v2封装到一个类中，作为新的k2
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
            return (this.first.equals(oK2.first)) && (this.second == oK2.second);
        }
    }

    static class MyGroupingComparator implements RawComparator<NewK2> {

        @Override
        public int compare(NewK2 o1, NewK2 o2) {
            // lexicographically 即按照字典序排序
            return (int)(o1.first.compareTo(o2.first));
        }

        /*
        * 其中b1为第一个对象所在的字节数组，s1为该对象在b1中的起始位置，l1为对象在b1中的长度，
        * b2为第一个对象所在的字节数组，s2为该对象在b2中的起始位置，l2为对象在b2中的长度。
         */
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

    private static void testORC() {
         final TypeInfo typeInfo = TypeInfoUtils
                .getTypeInfoFromTypeString("struct<fromip:string,request_time:timestamp,request_method:string,request_api:string,param:map<string,string>,http_version:string," +
                        "request_url:string,return_code:int,http_body_length:int,respone_time:decimal(19,4),respone_body_length:int,http_refer:string,user_agent:string,server_addr:string,upstream_addr:string,host:string,cacheflg:string>");

        System.out.println(typeInfo.toString());
        final ObjectInspector inspector = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(typeInfo);
        System.out.println(inspector);
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
//    testORC();

    }
}
