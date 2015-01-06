package CFRecommendation;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

import Recommendation.HDFS.Util;

public class CFRecommenderJob {
	/*
	 * Util.HDFS = "hdfs://192.168.75.133:9000";
	 * ItemIndex �̻�ID���Ͷ�Ӧ��ϸ��Ϣ
	 * InputPath ԭʼ�ļ�����·��
	 * OutPutPath �ļ����·��
	 * timesToscorePath �������ת���ֿ��˶��̻������ֱ�׼�����ļ�
	 */
	
	public static String ItemIndex=Util.HDFS+ "/CFRecommender/item";
	public static String InputPath=Util.HDFS+ "/CFRecommender/input/";
	public static String OutPutPath=Util.HDFS+"/CFRecommender/output";
	public static String timesToscorePath = "/CFRecommender/score/score";
	
	
	/*
	 * DEFAULT_NUM_RECOMMENDATIONS �Ƽ�����
	 * indexFilePath ����ɸѡ��������Ŀ¼
	 */
	public static final int DEFAULT_NUM_RECOMMENDATIONS = 20;
	public static String indexFilePath = "/CFRecommender/tmp/index/part-r-00000";
	


	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new Configuration(),
				new CFRecommenderDriver(), args));

	}
}
