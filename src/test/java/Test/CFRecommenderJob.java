package Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

public class CFRecommenderJob {
	
//	public static String ItemIndex= "/user/hddtmn/in_common_mchnt_inf/part-m-00000";
//	//public static String InputPath="/user/hddtmn/in_common_his_trans";
//	public static String InputPath="/user/hddtmn/tbl_common_his_trans_success/20130101/0001*/";
//	public static String OutPutPath="/CFRecommender/output";
//	public static String timesToscorePath = "/CFRecommender/score/score";
//	public static final int DEFAULT_NUM_RECOMMENDATIONS = 20;
//	public static String indexFilePath = "/CFRecommender/tmp/index/part-r-00000";
	


	public static void main(String[] args) throws Exception {
//		InputPath=args[0];
//		OutPutPath=args[1];
		System.exit(ToolRunner.run(new Configuration(),
				new CFRecommenderDriver(), args));

	}
}
