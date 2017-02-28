package com.juanpi.bi.merge;

import com.juanpi.bi.merge.util.HdfsUtil;
import org.apache.hadoop.fs.Path;
import org.joda.time.DateTime;

import java.io.IOException;
import java.util.*;

/**
 * 清理实时数据
 * Created by gongzi on 2017/1/11.
 */
public class CleanHistoricalData {

    String baseDir = "hdfs://nameservice1/user/hadoop/dw_realtime/dw_real_for_path_list";
    // 路径正则
    private String getDirRegex(String dateStr) {
        return baseDir + "/{mb_event_hash2,mb_pageinfo_hash2,pc_events_hash3,jp_hash3}/date=*";
    }

    // 获取匹配HDFS路径
    private Path[] getMatchDir(String srcDirRegex) throws IOException {
        // PathFilter是过滤布符合指定表达式的路径，ture：匹配part开头的数记录
        Path[] matchPaths = HdfsUtil.getHdfsFiles(srcDirRegex, null, true);
        return matchPaths;
    }

    /**
     * 删除指定文件
     * @param files
     * @throws IOException
     */
    private static void delete(List<Path> files) throws IOException {
        for (Path file : files) {
            System.out.println("delete small files:" + file.toString());
            HdfsUtil.delete(file);
        }
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

        // 指定日期的第7天之前
        DateTime startDate = new DateTime(dateStr).minusDays(10);

        // 明天
//            DateTime endDate = new DateTime().plusDays(1);

        // 今天
        DateTime curentDate = new DateTime();

        long startMillis = startDate.getMillis();

        // 当前的
        long curentMillis = curentDate.getMillis();

        ArrayList matchPath = new ArrayList();
        for (Path matchDir : matchDirs) {
            // hdfs 文件目录
            String name = matchDir.getName();
            String uriDtStr = name.substring(5);
            DateTime uriDate = new DateTime(uriDtStr);
            long uriMs = uriDate.getMillis();
            if(uriMs < startMillis || uriMs > curentMillis) {
                System.out.println("time matched path:" + matchDir.toUri());
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
        else {
            System.exit(1);
            System.out.println("CleanHistoricalData 参数错误！");
        }

        CleanHistoricalData chd = new CleanHistoricalData();

        try {
            chd.clean(dateStr);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
