package com.juanpi.bi.merge;

import java.io.IOException;
import java.text.ParseException;

import com.juanpi.bi.merge.util.DateUtil;
import com.juanpi.bi.merge.util.ProcessUtil;

/**
 * 
 * @author yunduan  
 * @date 2016年6月25日 下午1:20:38    
 * 任务管理
 */
public class TaskManager {
	
	private String date = null;
	private String gu_hash = null;
	private String baseDir = null;


	public TaskManager(String baseDir) {
		this.baseDir = baseDir;
		this.date = ProcessUtil.getBeforeOneHourDate();	// yyyy-mm-dd
		this.gu_hash = "*"; // HH
	}

	
	// 
	// 路径正则
	//	
	private String getDirRegex() {
		return baseDir + "mb_event_hash2/date=" + date + "/gu_hash=0/*";
	}
	
	/**
	 * 开始任务
	 * @throws IOException
	 */
	public void start() throws IOException {
        System.out.println("开始任务======>>....");
        String srcDirRegex = getDirRegex();
		MergeTask mergeTask = new MergeTask(srcDirRegex, null, true);
		mergeTask.doMerge();
	}
	
	public static void main(String[] args) {
        String dir = "hdfs://nameservice1/user/hadoop/gongzi/dw_real_for_path_list/";
        System.out.println("======>>main_dir:" + dir);
		TaskManager manager = new TaskManager(dir);
		try {
			manager.start();
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(-1);
		}
	}
}
