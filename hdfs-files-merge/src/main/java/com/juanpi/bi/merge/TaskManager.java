package com.juanpi.bi.merge;

import java.io.IOException;
import com.juanpi.bi.merge.util.ProcessUtil;

/**
 * 
 * @author yunduan
 * updated by gongzi
 * @date 2016年6月25日 下午1:20:38    
 * 任务管理
 */
public class TaskManager {

	private String baseDir = null;

	public TaskManager(String baseDir) {
		this.baseDir = baseDir;
	}

	// 路径正则 TODO 测试 2016-09-09 号数据
	private String getDirRegex(String dateStr) {
		return baseDir + "*/date=" + dateStr + "/gu_hash=*/";
	}
	
	/**
	 * 开始任务
	 * @throws IOException
	 */
	public void start(String dateStr) throws IOException {
        System.out.println("开始任务======>>....");
        String srcDirRegex = getDirRegex(dateStr);
		MergeTask mergeTask = new MergeTask(srcDirRegex, null, true);
		mergeTask.doMerge();
	}
	
	public static void main(String[] args) {

		// 传入 date 格式 yyyy-MM-dd
        // 传入 hdfs 目录

        String dateStr = "";
        String dir = "";

        System.out.println("args 参数个数：" + args.length);

		if(args.length == 1)
        {
            dateStr = args[0];
        }

        if(args.length == 2)
        {
            dateStr = args[0];
            dir = args[1];
        }

        if(null == dateStr || dateStr.isEmpty())
		{
            dateStr = ProcessUtil.getBeforeOneHourDate();
		}

        if(null == dir || dir.isEmpty())
        {
            dir = "hdfs://nameservice1/user/hadoop/gongzi/dw_real_for_path_list/";
        }

        System.out.println("======>>main_date:" + dateStr);
        System.out.println("======>>main_dir :" + dir);

		TaskManager manager = new TaskManager(dir);
		try {
			manager.start(dateStr);
		} catch (Exception e) {
            e.printStackTrace();
			System.exit(-1);
		}

        System.out.println("Small Files Merge Complete...");
    }
}
