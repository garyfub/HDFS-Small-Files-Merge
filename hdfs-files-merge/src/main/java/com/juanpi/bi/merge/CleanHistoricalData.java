package com.juanpi.bi.merge;

import com.juanpi.bi.merge.util.HdfsUtil;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.Arrays;

/**
 * 清理实时数据
 * Created by gongzi on 2017/1/11.
 */
public class CleanHistoricalData {

    String baseDir = "hdfs://nameservice1/user/hadoop/dw_realtime/dw_real_for_path_list";
    // 路径正则
    private String getDirRegex(String dateStr) {
        return baseDir + "/{mb_event_hash2,mb_pageinfo_hash2,pc_events_hash3}/date=*";
    }

    // 获取匹配HDFS路径
    private Path[] getMatchDir(String srcDirRegex) throws IOException {
        // PathFilter是过滤布符合指定表达式的路径，ture：匹配part开头的数记录
        Path[] matchPaths = HdfsUtil.getHdfsFiles(srcDirRegex, null, true);
        return matchPaths;
    }

    private void del(Path matchDir) {

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

        for (Path matchDir : matchDirs) {
            System.out.println("matchDir is:" + matchDir);
//            del(matchDir);
        }
    }

    public static void main(String[] args) {
        String dateStr = "";

        System.out.println("args 参数个数：" + args.length);

        if(args.length == 1)
        {
            dateStr = args[0];
        }


        CleanHistoricalData chd = new CleanHistoricalData();
        try {
            chd.clean(dateStr);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
