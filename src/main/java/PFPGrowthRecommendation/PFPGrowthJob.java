package PFPGrowthRecommendation;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

import Recommendation.HDFS.Util;


public class PFPGrowthJob {
	//相关设置 最小组，分符，组，输入，输出路径
	public static int numGroups=6;
	public static int minSupport=8;
	public static String splitter=",";
	public static String InPut="/fprecommender/input/data";
	public static String OutPut="/fprecommender/frequentpatterns";
	
	
	public static String InputPath=Util.HDFS+InPut;
	public static String OutputPath=Util.HDFS+OutPut;
	
	//中间结果路径
	public static String SavefListPath=Util.HDFS+"/fprecommender/tmp/fList";
	public static String CountingOutPath=Util.HDFS+ "/fprecommender/tmp/parallelcounting";
	public static String StartingGrowthOutPath=Util.HDFS + "/fprecommender/tmp/fpgrowth";
	
	public static void main(String[] args)throws Exception{
		System.exit(ToolRunner.run(new Configuration(),
				new PFPGrowthDriver() , args));
	}
}
