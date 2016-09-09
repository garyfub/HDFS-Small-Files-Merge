package com.juanpi.bi.merge;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.UUID;

import com.google.common.base.Joiner;
import com.juanpi.bi.merge.util.DateUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.juanpi.bi.merge.util.HdfsUtil;
import com.juanpi.bi.merge.util.ProcessUtil;

/**
 * 
 * @author yunduan  
 * @date 2016年6月25日 下午12:59:29    
 * 合并HDFS小文件
 */
public class MergeTask {
	
	private static Configuration configuration = new Configuration();
	
	private String srcDirRegex = null;
	private String dstFileName = null;
	private boolean deleteSource = false;
	
	private FileSystem fs = null;

    public static long oneHourAgoMillis = getHoursAgoMillis();

    private static long getHoursAgoMillis()
    {
        Calendar cal = Calendar.getInstance();
        long milis = cal.getTimeInMillis();
        String fmt = "yyyy-MM-dd HH:00:00";
        String dt = DateUtil.getHourIntervalDate(-18, fmt);
        try {
            milis = DateUtil.dateToMillis(dt, fmt);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return milis;
    }

    /**
	 * 
	 * @param srcDirRegex 目录通配符
	 * @param dstFileName 目标文件名
	 * @param deleteSource 是否删除原始文件
	 * @throws IOException
	 */
	public MergeTask(String srcDirRegex, String dstFileName, boolean deleteSource) throws IOException {
		this.srcDirRegex = srcDirRegex;
		this.dstFileName = dstFileName;
		this.deleteSource = deleteSource;
		
		this.fs = FileSystem.get(configuration);
	}
	
	//
	// 获取匹配HDFS路径
	//
	private Path[] getMatchDir() throws IOException {
		Path[] matchPaths = HdfsUtil.getHdfsFiles(srcDirRegex, ".*", true);
		return matchPaths;
	}
	
	//
	// 获取目录下所有文件
	//
	private Path[] getFile(Path dir) throws IOException {
		return HdfsUtil.getHdfsFiles(dir);
	}
	
	//
	// 删除指定文件
	//
	private void delete(Path[] files) throws IOException {
		for (Path file : files) {
			HdfsUtil.delete(file);
		}
	}

	//
	// merge目标文件路径
	//
	private Path getDstFile(Path srcDir) {
		StringBuilder dstFileBuf = new StringBuilder();

        String fileName = srcDir.getName();
        // 文件名格式：part_1473411900000
        String timeMillis = fileName.split("_")[1];
        String dateHourStr = DateUtil.dateHourStr(timeMillis, "yyyyMMddHH");

        dstFileBuf.append(srcDir.getParent().toString());
        dstFileBuf.append("/merged_" + dateHourStr);
		Path dstFile = new Path(dstFileBuf.toString());

		return dstFile;
	}
	
	//
	// merge小文件
	//
	private void merge(Path srcDir, Path dstFile, boolean deleteSource) throws IOException {
		boolean res = HdfsUtil.copyMerge(fs, srcDir, fs, dstFile, deleteSource, configuration, null);
        System.out.println("merge res=" + res);
    }
	
	public void doMerge() throws IOException {

        System.out.println("doMerge start======>>....");
        Path[] matchDirs = getMatchDir();
		if (matchDirs == null || matchDirs.length < 1) {
            System.out.println("matchDirs is null....");
            return;
		}
		
		for (Path matchDir : matchDirs) {
			Path[] files = getFile(matchDir);

            // 文件名格式：part_1473411900000
            String fileName = matchDir.getName();
            String timeMillis = fileName.split("_")[1];
            long millis = Long.parseLong(timeMillis);
            if(millis <= oneHourAgoMillis)
            {
                Path dstFile = getDstFile(matchDir);
                System.out.println("dstFile=======>>" + dstFile.toString());

                merge(matchDir, dstFile, false);

    //			if (deleteSource) {
    //				delete(files);
    //			}
            }

		}
	}
}
