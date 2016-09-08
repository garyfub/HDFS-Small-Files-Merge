package com.juanpi.bi.merge.util;

import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.regex.Pattern;

import com.google.common.base.Joiner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

/**
 * 
 * @author yunduan  
 * @date 2016年6月23日 上午10:54:17    
 * 
 */
public class HdfsUtil extends FileUtil {
	
	private static Configuration configuration = new Configuration();
	
	private static class RegexPathFilter implements PathFilter {
		private String regex;
		private boolean isInclude = true;
		
		public RegexPathFilter(String regex, boolean isInclude) {
			this.regex = regex;
			this.isInclude = isInclude;
		}
		
		public boolean accept(Path path) {
			if (isInclude) {
				return Pattern.compile(regex).matcher(path.toString()).matches();
			}
			return !Pattern.compile(regex).matcher(path.toString()).matches();
		}
	}
	
	/**
	 * 
	 * @param hdfsDirRegex HDFS目录正则
	 * @param regex 正则
	 * @param isInclude true 输出匹配正则HDFS文件路径, false输出不匹配正则HDFS路径
	 * @return HDFS路径集合
	 * @throws IOException
	 */
	public static Path[] getHdfsFiles(String hdfsDirRegex, String regex, boolean isInclude) throws IOException {
		FileSystem fs = FileSystem.get(configuration);
		System.out.println("hdfsDirRegex is:" + hdfsDirRegex);
		FileStatus[] files = fs.globStatus(new Path(hdfsDirRegex), new RegexPathFilter(regex, isInclude));
		System.out.println("getHdfsFiles are:" + Joiner.on(";").join(files));
		return FileUtil.stat2Paths(files);
	}
	
	/**
	 * 
	 * @param directory 指定任务输入目录
	 * @return path[]
	 * @throws IOException
	 */
	public static Path[] getInputPaths(String directory) throws IOException {
		List<Path> pathList = new LinkedList<Path>();
		for (String current : directory.split(",")) {
			if (ProcessUtil.isNull(current)) {
				continue;
			}
			Path[] paths = getHdfsFiles(new Path(current));
			for (Path path : paths) {
				pathList.add(path);
			}
		}
		Path[] pathArr = new Path[pathList.size()];
		int index = 0;
		for (Path path : pathList) {
			pathArr[index] = path;
			index++;
		}
		
		return pathArr;
	}
	
    /**
     * 获取hdfs指定目录下所有文件
     */
    public static Path[] getHdfsFiles(Path dir) throws IOException {
        FileSystem fs = FileSystem.get(configuration);
        FileStatus[] files = fs.listStatus(dir);
        
        return FileUtil.stat2Paths(files);
    }
	
	/**
	 * 
	 * @param directory 目录
	 * @return 文件集合
	 */
	public static List<File> getLocalFiles(String directory) {
		List<File> fileStorage = new LinkedList<File>();
		List<File> dirStorage = new LinkedList<File>();
		
		for (File file : new File(directory).listFiles()) {
			dirStorage.add(file);
		}
		
		while (true) {
			List<File> currentDirStorage = new LinkedList<File>();
			for (File currentFile : dirStorage) {
				if (currentFile.isFile()) {
					fileStorage.add(currentFile);
				} else {
					for (File currentFileList : currentFile.listFiles()) {
						currentDirStorage.add(currentFileList);
					}
				}
			}
			
			if (currentDirStorage.size() == 0) {
				break;
			} 
			
			dirStorage = currentDirStorage;
		}
		
		return fileStorage;
	}
	
    /**
     * 
     * 删除hdfs指定目录或文件
     */
    public static void delete(Path file) throws IOException {
        FileSystem fs = FileSystem.get(configuration);
        fs.delete(file, true);
    }
    
    /**
     * 
     * @param localDir 本地目录
     * @param hdfsDir HDFS目录
     * @return 成功返回true,反之false
     */
    public static boolean copyLocalFileToHdfs(String localDir, String hdfsDir) {
    	try {
    		List<File> localFiles = getLocalFiles(localDir);
    		if (ProcessUtil.isNull(localFiles)) {
    			return false;
    		}
    		
        	FileSystem fs = FileSystem.get(configuration);
        	for (File localFile : localFiles) {
        		copy(localFile, fs, new Path(hdfsDir + localFile.getName()), false, configuration);
        	}
        	
        	return true;
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
    }
    
    /**
     * 
     * @param hdfsDir HDFS目录
     * @param localDir 本地目录
     * @return 成功返回true,反之false
     */
    public static List<File> copyHdfsToLocal(String hdfsDir, String localDir) {
    	try {
			Path[] paths = getHdfsFiles(new Path(hdfsDir));
			if (paths == null || paths.length == 0) {
				return null;
			}
			
			List<File> files = new LinkedList<File>();
			FileSystem fs = FileSystem.get(configuration);
			for (Path path : paths) {
				File localFile = new File(localDir + UUID.randomUUID());
				copy(fs, path, localFile, false, configuration);
				files.add(localFile);
			}
			
			if (ProcessUtil.isNull(files)) {
				return null;
			}
			
			return files;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
    }
    
}
