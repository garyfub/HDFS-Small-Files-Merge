package com.juanpi.bi.merge;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.text.ParseException;
import java.util.*;

import com.google.common.base.Joiner;
import com.juanpi.bi.merge.util.DateUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import com.juanpi.bi.merge.util.HdfsUtil;
import org.apache.hadoop.io.IOUtils;

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
        String dt = DateUtil.getHourIntervalDate(0, fmt);
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
	
	// 获取匹配HDFS路径
	private Path[] getMatchDir() throws IOException {
        // PathFilter是过滤布符合指定表达式的路径，ture：匹配part开头的数记录
		Path[] matchPaths = HdfsUtil.getHdfsFiles(this.srcDirRegex, "part.*", false);
        System.out.println("getMatchDir==" + Joiner.on(",").join(matchPaths));
        return matchPaths;
	}

	// merge目标文件路径
	private static Path getDstFile(Path srcDir) {
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

    // 删除指定文件
    private static void delete(Path[] files) throws IOException {
        for (Path file : files) {
            HdfsUtil.delete(file);
        }
    }

    /**
     *
     * @param srcFS hdfs 文件系统
     * @param srcDir 文件目录
     * @param deleteSource 是否删除
     * @param conf hadoop配置文件
     * @throws IOException
     */
	public static void copyLogMerge(FileSystem srcFS, Path srcDir, boolean deleteSource, Configuration conf) throws IOException {

		FileStatus[] fileStatus = srcFS.listStatus(srcDir);
        List<Path> mergingFiles = new ArrayList<>();

        for (FileStatus fileStat : fileStatus) {
			Path logfile = fileStat.getPath();

			// 文件名格式：part_1473411900000
			String fileName = logfile.getName();
			String timeMillis = fileName.split("_")[1];
			long millis = Long.parseLong(timeMillis);

			if (millis <= oneHourAgoMillis) {
                mergingFiles.add(logfile);
				Path dstPath = getDstFile(logfile);

                // true：overwrite
                OutputStream out = srcFS.create(dstPath, false);
				try {
					InputStream in = srcFS.open(logfile);
					try {
						IOUtils.copyBytes(in, out, conf, false);
					} finally {
						in.close();
					}
				} finally {
					out.close();
				}
			}
		}

        if (deleteSource) {
            if(mergingFiles.size() > 0)
            {
                // 强制类型转换
                Path[] delFiles = (Path[]) mergingFiles.toArray();
                delete(delFiles);
            }
        }
	}
	
	// merge小文件
	private void merge(Path srcDir, boolean deleteSource) throws IOException {

        if(!fs.getFileStatus(srcDir).isDirectory()) {
            System.out.println("srcDir is not directory");
        }

		copyLogMerge(fs, srcDir, deleteSource, configuration);
    }
	
	public void doMerge() throws IOException {

        System.out.println("doMerge start======>>....");
        Path[] matchDirs = getMatchDir();

		if (matchDirs == null || matchDirs.length < 1) {
            System.out.println("matchDirs is null....");
            return;
		}
		
		for (Path matchDir : matchDirs) {
			merge(matchDir, false);
		}
	}
}
