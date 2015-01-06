package org.bai.hadoopTest;


import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.mahout.cf.taste.hadoop.RecommendedItemsWritable;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;

import CFRecommendation.Util.CFRecommendationUtil;
import Recommendation.HDFS.Util;



public class TestConfiguration {
	public static void main(String[] args) throws Exception {
		Configuration conf=new Configuration();
		//FileSystem fs=FileSystem.get(conf);
		Job job0=new Job(conf,"step0");
		job0.setJarByClass(TestConfiguration.class);
		job0.setMapperClass(MaxMap.class);
		job0.setReducerClass(Reducer.class);
		
		job0.setMapOutputKeyClass(Text.class);
		job0.setMapOutputValueClass(Text.class);

		job0.setOutputKeyClass(Text.class);
		job0.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job0, new Path( Util.HDFS+"/recommender/output/part-r-00000"));
		FileOutputFormat.setOutputPath(job0, new Path(Util.HDFS+"/test/"));
//		
//		job0.setInputFormatClass(SequenceFileInputFormat.class);
//		job0.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		System.exit(job0.waitForCompletion(true) ? 0 : 1);
	}
	
	public static class MaxMap extends Mapper<VLongWritable, RecommendedItemsWritable,Text ,Text>
	{
		public void map(LongWritable key, RecommendedItemsWritable value,Context context)throws IOException, InterruptedException {
			String USERID="user"+key.get();
			List<RecommendedItem> ris=value.getRecommendedItems();
			for(RecommendedItem re:ris){
				System.out.println(re);
				String reim=re.toString();
				context.write(new Text(USERID), new Text(reim));
			}
		}
	}

	
}
