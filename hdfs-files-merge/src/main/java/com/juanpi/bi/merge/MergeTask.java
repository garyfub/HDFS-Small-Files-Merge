package com.juanpi.bi.merge;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;

import com.juanpi.bi.merge.utils.DateUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import com.juanpi.bi.merge.utils.HdfsUtil;
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
	private String sourceDir = "";
	private String targetDir = "";
	private boolean deleteSource = false;
    private long oneHourAgoMillis = 0;
	
	private FileSystem fs = null;

    /**
     *
     * @param srcDirRegex 目录通配符
     * @param sourceDir 源目录名
     * @param targetDir 目标文件名
     * @param deleteSource 是否删除原始文件
     * @param oneHourAgoMillis
     * @throws IOException
     */
	public MergeTask(String srcDirRegex, String sourceDir, String targetDir, boolean deleteSource, long oneHourAgoMillis) throws IOException {
		this.srcDirRegex = srcDirRegex;
		this.sourceDir = sourceDir;
		this.targetDir = targetDir;
		this.deleteSource = deleteSource;
		this.oneHourAgoMillis = oneHourAgoMillis;
		this.fs = FileSystem.get(configuration);
	}
	
	// 获取匹配HDFS路径
	private Path[] getMatchDir() throws IOException {
        // PathFilter是过滤布符合指定表达式的路径，ture：匹配part开头的数记录
		Path[] matchPaths = HdfsUtil.getHdfsFiles(this.srcDirRegex, "part.*", false);
        return matchPaths;
	}

    // 删除指定文件
    private static void delete(List<Path> files) throws IOException {
        for (Path file : files) {
            HdfsUtil.delete(file);
        }
    }

    private String getSmallFilePath(String source) {
	    String path = "";
	    return path;
    }

    private String getOutPutPath() {
	    String path = "";
	    return path;
    }

    /**
     *
     * @param srcFS hdfs 文件系统
     * @param srcDir 文件目录
     * @param deleteSource 是否删除
     * @param conf hadoop配置文件
     * @throws IOException
     */
	private void copyLogMerge(FileSystem srcFS, Path srcDir, boolean deleteSource, Configuration conf) throws IOException {

		FileStatus[] fileStatus = srcFS.listStatus(srcDir);

        HashMap<String, List<Path>> filesMap = new HashMap<>();

        for (FileStatus fileStat : fileStatus) {
            Path logfile = fileStat.getPath();

            // 文件名格式：part_1473411900000
            String fileName = logfile.getName();
            String timeMillis = fileName.split("_")[1];

            long millis = Long.parseLong(timeMillis);

            if (fileName.startsWith("part_") && millis < oneHourAgoMillis) {

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

        // 判断是否合并文件,如果源数据目录与目标目录相同，就是合并重新消费的文件
        boolean dirFlag = false;
        if(this.sourceDir.equals(this.targetDir)) {
            dirFlag = true;
        }

        Iterator iter = filesMap.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry entry = (Map.Entry) iter.next();
            String dateHourStr = (String) entry.getKey();
            List<Path> mergingFiles = (List<Path>) entry.getValue();

            // 如果存在需要合并的小文件
            if(mergingFiles.size() > 0)
            {
                StringBuilder dstFileBuf = new StringBuilder();

                // 创建目标文件 新目录
                Path fileParnt = mergingFiles.get(0).getParent();
                dstFileBuf.append(fileParnt.getParent().toString());
                dstFileBuf.append("/merged/merged_" + dateHourStr);

                Path dstPath;

                // 重复消费时用到
                if(dirFlag == true)
                {
                    String path = dstFileBuf.toString();
                    // replace(CharSequence target, CharSequence replacement)
                    // target The sequence of char values to be replaced
                    // replacement The replacement sequence of char values
                    // 将源数据合并后移动到新的目录
                    String targetPath = path.replace(this.sourceDir, this.targetDir);
                    dstPath = new Path(targetPath);
                }
                else {
                    dstPath = new Path(dstFileBuf.toString());
                }

                OutputStream out = srcFS.create(dstPath);
                try
                {
                    // 遍历小文件, 复制
                    for (Path logfile : mergingFiles) {
                        InputStream in = srcFS.open(logfile);
                        try {
                            //* @param in InputStream to read from
                            //* @param out OutputStream to write to
                            //* @param conf the Configuration object
                            //* @param close whether or not close the InputStream and
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
//                        System.out.println("删除小文件：" + dateHourStr + ",目录：" + mergingFiles.get(0).getParent());
                        delete(mergingFiles);
                    }
                }
            }
        }
	}

    /**
     *
     * @param srcDir
     * @param deleteSource
     * @throws IOException
     */
	private void merge(Path srcDir, boolean deleteSource) throws IOException {

        if(!fs.getFileStatus(srcDir).isDirectory()) {
            System.out.println("srcDir is not directory");
        }

		copyLogMerge(fs, srcDir, deleteSource, configuration);
    }

    /**
     *
     * @throws IOException
     */
	public void doMerge() throws IOException {

        System.out.println("doMerge start======>>....");
        Path[] matchDirs = getMatchDir();

		if (matchDirs == null || matchDirs.length < 1) {
            System.out.println("matchDirs is null....");
            return;
		}

        Arrays.sort(matchDirs);
		
		for (Path matchDir : matchDirs) {
            System.out.println("matchDir is:" + matchDir);
            merge(matchDir,true);
		}
	}
}
