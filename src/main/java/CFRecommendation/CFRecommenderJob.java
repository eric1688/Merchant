package CFRecommendation;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

import Recommendation.HDFS.Util;

public class CFRecommenderJob {
	/*
	 * Util.HDFS = "hdfs://192.168.75.133:9000";
	 * ItemIndex 商户ID，和对应详细信息
	 * InputPath 原始文件输入路径
	 * OutPutPath 文件输出路径
	 * timesToscorePath 购买次数转化持卡人对商户的评分标准输入文件
	 */
	
	public static String ItemIndex=Util.HDFS+ "/CFRecommender/item";
	public static String InputPath=Util.HDFS+ "/CFRecommender/input/";
	public static String OutPutPath=Util.HDFS+"/CFRecommender/output";
	public static String timesToscorePath = "/CFRecommender/score/score";
	
	
	/*
	 * DEFAULT_NUM_RECOMMENDATIONS 推荐个数
	 * indexFilePath 数据筛选清理后输出目录
	 */
	public static final int DEFAULT_NUM_RECOMMENDATIONS = 20;
	public static String indexFilePath = "/CFRecommender/tmp/index/part-r-00000";
	


	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new Configuration(),
				new CFRecommenderDriver(), args));

	}
}
