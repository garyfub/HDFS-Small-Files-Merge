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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class SecondarySort
{
    //自己定义的key类应该实现WritableComparable接口
    public static class IntPair implements WritableComparable<IntPair>
    {
        String first;
        String second;
        /**
         * Set the left and right values.
         */
        public void set(String left, String right)
        {
            first = left;
            second = right;
        }
        public String getFirst()
        {
            return first;
        }
        public String getSecond()
        {
            return second;
        }
        //反序列化，从流中的二进制转换成IntPair
        public void readFields(DataInput in) throws IOException
        {
            first = in.readUTF();
            second = in.readUTF();
        }
        //序列化，将IntPair转化成使用流传送的二进制
        public void write(DataOutput out) throws IOException
        {
            out.writeUTF(first);
            out.writeUTF(second);
        }
        //重载 compareTo 方法，进行组合键 key 的比较，该过程是默认行为。
        //分组后的二次排序会隐式调用该方法。
        public int compareTo(IntPair o)
        {
            if (!first.equals(o.first) )
            {
                return first.compareTo(o.first);
            }
            else if (!second.equals(o.second))
            {
                return second.compareTo(o.second);
            }
            else
            {
                return 0;
            }
        }

        //新定义类应该重写的两个方法
        //The hashCode() method is used by the HashPartitioner (the default partitioner in MapReduce)
        public int hashCode()
        {
            return first.hashCode() * 157 + second.hashCode();
        }
        public boolean equals(Object right)
        {
            if (right == null)
                return false;
            if (this == right)
                return true;
            if (right instanceof IntPair)
            {
                IntPair r = (IntPair) right;
                return r.first.equals(first) && r.second.equals(second) ;
            }
            else
            {
                return false;
            }
        }
    }
    /**
     * 分区函数类。根据first确定Partition。
     */
    public static class FirstPartitioner extends Partitioner<IntPair, Text>
    {
        public int getPartition(IntPair key, Text value,int numPartitions)
        {
            return Math.abs(key.getFirst().hashCode() * 127) % numPartitions;
        }
    }

    /**
     * 分组函数类。只要first相同就属于同一个组。
     */
    /*//第一种方法，实现接口RawComparator
    public static class GroupingComparator implements RawComparator<IntPair> {
        public int compare(IntPair o1, IntPair o2) {
            int l = o1.getFirst();
            int r = o2.getFirst();
            return l == r ? 0 : (l < r ? -1 : 1);
        }
        //一个字节一个字节的比，直到找到一个不相同的字节，然后比这个字节的大小作为两个字节流的大小比较结果。
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2){
             return WritableComparator.compareBytes(b1, s1, Integer.SIZE/8,
                     b2, s2, Integer.SIZE/8);
        }
    }*/
    //第二种方法，继承WritableComparator
    public static class GroupingComparator extends WritableComparator
    {
        protected GroupingComparator()
        {
            super(IntPair.class, true);
        }
        //Compare two WritableComparables.
        //  重载 compare：对组合键按第一个自然键排序分组
        public int compare(WritableComparable w1, WritableComparable w2)
        {
            IntPair ip1 = (IntPair) w1;
            IntPair ip2 = (IntPair) w2;
            String l = ip1.getFirst();
            String r = ip2.getFirst();
            return l.compareTo(r);
        }
    }


    // 自定义map
    public static class Map extends Mapper<LongWritable, Text, IntPair, Text>
    {
        private final IntPair keyPair = new IntPair();
        String[] lineArr = null;
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {
            String line = value.toString();
            lineArr = line.split("\t", -1);
            keyPair.set(lineArr[0], lineArr[1]);
            context.write(keyPair, value);
        }
    }
    // 自定义reduce
    //
    public static class Reduce extends Reducer<IntPair, Text, Text, Text>
    {
        private static final Text SEPARATOR = new Text("------------------------------------------------");

        public void reduce(IntPair key, Iterable<Text> values,Context context) throws IOException, InterruptedException
        {
            context.write(SEPARATOR, null);
            for (Text val : values)
            {
                context.write(null, val);
            }
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException
    {
        // 读取hadoop配置
        Configuration conf = new Configuration();
        // 实例化一道作业
        Job job = new Job(conf, "secondarysort");
        job.setJarByClass(SecondarySort.class);
        // Mapper类型
        job.setMapperClass(Map.class);
        // 不再需要Combiner类型，因为Combiner的输出类型<Text, IntWritable>对Reduce的输入类型<IntPair, IntWritable>不适用
        //job.setCombinerClass(Reduce.class);
        // Reducer类型
        job.setReducerClass(Reduce.class);
        // 分区函数
        job.setPartitionerClass(FirstPartitioner.class);
        // 分组函数
        job.setGroupingComparatorClass(GroupingComparator.class);

        // map 输出Key的类型
        job.setMapOutputKeyClass(IntPair.class);
        // map输出Value的类型
        job.setMapOutputValueClass(Text.class);
        // rduce输出Key的类型，是Text，因为使用的OutputFormatClass是TextOutputFormat
        job.setOutputKeyClass(Text.class);
        // rduce输出Value的类型
        job.setOutputValueClass(Text.class);

        // 将输入的数据集分割成小数据块splites，同时提供一个RecordReder的实现。
        job.setInputFormatClass(TextInputFormat.class);
        // 提供一个RecordWriter的实现，负责数据输出。
        job.setOutputFormatClass(TextOutputFormat.class);

        // 输入hdfs路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        // 输出hdfs路径
        FileSystem.get(conf).delete(new Path(args[1]), true);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        // 提交job
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}