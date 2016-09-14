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

        HashMap<String, List<Path>> filesMap = new HashMap<>();

        for (FileStatus fileStat : fileStatus) {
            Path logfile = fileStat.getPath();

            // 文件名格式：part_1473411900000
            String fileName = logfile.getName();
            String timeMillis = fileName.split("_")[1];

            long millis = Long.parseLong(timeMillis);

            if (fileName.startsWith("part_") && millis <= oneHourAgoMillis) {

                String dateHourStr = DateUtil.dateHourStr(timeMillis, "yyyyMMddHH");

                // 如果key对应的ArrayList不存在，就创建ArrayList
                if(null == filesMap.get(dateHourStr) || filesMap.get(dateHourStr).isEmpty())
                {
                    List<Path> mergingFiles = new ArrayList<>();
                    mergingFiles.add(logfile);
                    filesMap.put(dateHourStr, mergingFiles);
                }
                else
                {
                    // 如果key对应的ArrayList存在就，直接追加
                    filesMap.get(dateHourStr).add(logfile);
                }
            }
        }

        Iterator iter = filesMap.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry entry = (Map.Entry) iter.next();
            String dateHourStr = (String) entry.getKey();
            List<Path> mergingFiles = (List<Path>) entry.getValue();

            // 如果存在需要合并的小文件
            if(mergingFiles.size() > 0)
            {
//                for (Path logfile : mergingFiles) {
//                    System.out.println("======>> mergingFiles is:" + logfile.toString());
//                }

                StringBuilder dstFileBuf = new StringBuilder();

                // 创建目标文件
                dstFileBuf.append(mergingFiles.get(0).getParent().toString());
                dstFileBuf.append("/merged_" + dateHourStr);
                Path dstPath = new Path(dstFileBuf.toString());

                OutputStream out = srcFS.create(dstPath);
                try
                {
                    // 遍历小文件
                    for (Path logfile : mergingFiles) {
                        InputStream in = srcFS.open(logfile);
                        try {
                            IOUtils.copyBytes(in, out, conf, false);
                        } finally {
                            in.close();
                        }
                    }
                }
                finally
                {
                    out.close();
                }

                // 合并后，删除小文件
                if (deleteSource) {
                    if(mergingFiles.size() > 0)
                    {
                        // 强制类型转换
                        Path[] delFiles = (Path[]) mergingFiles.toArray();
                        delete(delFiles);
                    }
                }
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
            System.out.println("matchDir is:" + matchDir);
            merge(matchDir, false);
		}
	}
}
