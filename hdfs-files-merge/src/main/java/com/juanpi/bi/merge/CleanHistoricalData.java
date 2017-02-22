package com.juanpi.bi.merge;

import com.juanpi.bi.merge.util.DateUtil;
import com.juanpi.bi.merge.util.HdfsUtil;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

/**
 * 清理实时数据
 * Created by gongzi on 2017/1/11.
 */
public class CleanHistoricalData {

    String baseDir = "hdfs://nameservice1/user/hadoop/dw_realtime/dw_real_for_path_list";
    // 路径正则
    private String getDirRegex(String dateStr) {
        return baseDir + "/{bi_gongzi_mb_event_real_direct_by_dw,bi_gongzi_mb_pageinfo_real_direct_by_dw,bi_gongzi_pc_events_hash3_real_by_dw,bi_gongzi_jp_hash3_real_by_dw}/date=*";
    }

    // 获取匹配HDFS路径
    private Path[] getMatchDir(String srcDirRegex) throws IOException {
        // PathFilter是过滤布符合指定表达式的路径，ture：匹配part开头的数记录
        Path[] matchPaths = HdfsUtil.getHdfsFiles(srcDirRegex, null, true);
        return matchPaths;
    }

    // 删除指定文件
    private static void delete(List<Path> files) throws IOException {
        for (Path file : files) {

            System.out.println(file);
//            HdfsUtil.delete(file);
        }
    }

    /**
     * 返回时间对应的毫秒
     * @param dateStr
     * @return
     */
    private long dateFormatString(String dateStr) {
        DateFormat df = new SimpleDateFormat("yyyyMMdd");

        Date dt;
        long miliSeconds = 0;
        try {
            dt = df.parse(dateStr);
            miliSeconds = dt.getTime();
        } catch (ParseException e) {
            e.printStackTrace();
        }

        return miliSeconds;
    }

    /**
     * 清理目录
     * @throws IOException
     * @param dateStr
     */
    private void clean(String dateStr) throws IOException {

        String srcDirRegex = getDirRegex(dateStr);
        Path[] matchDirs = getMatchDir(srcDirRegex);

        if (matchDirs == null || matchDirs.length < 1) {
            System.out.println("matchDirs is null....");
            return;
        }

        // 排序
        Arrays.sort(matchDirs);

        // 传入时间
        String ds = DateUtil.getSpecifiedDayAgo(dateStr, 7);
        long curMs = dateFormatString(ds);

        ArrayList matchPath = new ArrayList();
        for (Path matchDir : matchDirs) {
//            System.out.println("find matchDir:" + matchDir);

            // date=2017-01-01
            String name = matchDir.getName();
            String uriDtStr = name.substring(5);
            // 从uri中解析出来的时间
            long uriMs = dateFormatString(uriDtStr);
            if(uriMs < curMs) {
                matchPath.add(matchDir);
            }
        }

        delete(matchPath);
    }

    public static void main(String[] args) {
        String dateStr = "";

        System.out.println("args 参数个数：" + args.length);

        if(args.length == 1)
        {
            dateStr = args[0];
        }

        CleanHistoricalData chd = new CleanHistoricalData();

//        String path = new Path("hdfs://nameservice1/user/hadoop/dw_realtime/dw_real_for_path_list/pc_events_hash3/date=2017-01-12").getName();
//        System.out.println(path);
//
//        long time = chd.dateFormatString(path.substring(5));
//        System.out.println(time);
//        System.exit(11);

        try {
            chd.clean(dateStr);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
