package com.juanpi.bi.merge;

import java.io.IOException;
import java.util.UUID;

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
		String srcDirStr = srcDir.toString();
		StringBuilder dstFileBuf = new StringBuilder();
		dstFileBuf.append(srcDirStr);
		if (!srcDirStr.equals("/")) {
			dstFileBuf.append("/");
		}
		
		if (!ProcessUtil.isNull(dstFileName)) {
			dstFileBuf.append(dstFileName).append("-");
		}

		dstFileBuf.append(UUID.randomUUID().toString());
		
		Path dstFile = new Path(dstFileBuf.toString());
		return dstFile;
	}
	
	//
	// merge小文件
	//
	private void merge(Path srcDir, Path dstFile, boolean deleteSource) throws IOException {
		HdfsUtil.copyMerge(fs, srcDir, fs, dstFile, deleteSource, configuration, null);
	}
	
	public void doMerge() throws IOException {
        System.out.println("doMerge start======>>....");
        Path[] matchDirs = getMatchDir();
		if (matchDirs == null || matchDirs.length < 1) {
            System.out.println("matchDirs is null....");
            return;
		}
		
		for (Path matchDir : matchDirs) {
			System.out.println(matchDir.toString());
			Path[] files = getFile(matchDir);
			System.out.println("=======>> matchDir:" + matchDir);

			Path dstFile = getDstFile(matchDir);
			System.out.println("=======>> dstFile:" + dstFile);
//			merge(matchDir, dstFile, false);
			
//			if (deleteSource) {
//				delete(files);
//			}
		}
	}
}
