package org.bai.hadoop.split;

import org.apache.commons.io.output.NullWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.mahout.common.HadoopUtil;

import Recommendation.HDFS.Util;


public class CFRecommenderDriver extends Configured implements Tool {

	public int run(String[] args) throws Exception {

		Configuration conf0 = new Configuration();
		Job job0 = new Job(conf0, "step0");
		job0.setJarByClass(CFRecommenderDriver.class);
		job0.setMapperClass(Mapper.class);
		job0.setReducerClass(Reducer.class);

		job0.setMapOutputKeyClass(LongWritable.class);
		job0.setMapOutputValueClass(Text.class);

		job0.setOutputKeyClass(LongWritable.class);
		job0.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job0, new Path(Util.HDFS+"/hddtmn/doc"));
		Path outPath0 = new Path(Util.HDFS+"/hddtmn/output");
		FileOutputFormat.setOutputPath(job0, outPath0);
		HadoopUtil.delete(conf0, new Path[] { outPath0 });
		
		job0.setNumReduceTasks(2);
		job0.waitForCompletion(true);

//		Configuration conf1 = new Configuration();
//
//		Job job1 = new Job(conf1, "step1");
//		job1.setJarByClass(CFRecommenderDriver.class);
//
//		job1.setMapperClass(SourcedataToUSPrefMapper.class);
//		job1.setReducerClass(Reducer.class);
//		 //job1.setReducerClass(Reducer.class);
//		//15370286069008784
//		job1.setMapOutputKeyClass(VLongWritable.class);
//		job1.setMapOutputValueClass(VLongWritable.class);
//
//		job1.setOutputKeyClass(VLongWritable.class);
//		job1.setOutputValueClass(VLongWritable.class);
//
//		//job1.setOutputFormatClass(SequenceFileOutputFormat.class);
//		job1.setOutputFormatClass(TextOutputFormat.class);
//		FileInputFormat.addInputPath(job1, new Path("/user/hddtmn/tbl_common_his_trans_success/20130101/0001*/"));
//		Path outPath1 = new Path("/CFRecommender/tmp/step15/step10");
//		FileOutputFormat.setOutputPath(job1, outPath1);
	//	HadoopUtil.delete(conf1, new Path[] { outPath1 });
		
//		job1.setNumReduceTasks(6);
//		job1.waitForCompletion(true);

//		Configuration conf2 = new Configuration();
//
//		Job job2 = new Job(conf2, "step2");
//		job2.setJarByClass(CFRecommenderDriver.class);
//
//		job2.setMapperClass(UserVectorToCooccurrenceMapper.class);
//		job2.setReducerClass(UserVectorToCooccurrenceReducer.class);
//
//		job2.setMapOutputKeyClass(IntWritable.class);
//		job2.setMapOutputValueClass(IntWritable.class);
//
//		job2.setOutputKeyClass(IntWritable.class);
//		job2.setOutputValueClass(VectorOrPrefWritable.class);
//
//		FileInputFormat.addInputPath(job2, new Path("/CFRecommender/tmp/step15/step1"));
//		Path outPath2 = new Path("/CFRecommender/tmp/step23/step2");
//		FileOutputFormat.setOutputPath(job2, outPath2);
//
//		HadoopUtil.delete(conf2, new Path[] { outPath2 });
//		job2.setInputFormatClass(SequenceFileInputFormat.class);
//		job2.setOutputFormatClass(SequenceFileOutputFormat.class);
//
//		 job2.setNumReduceTasks(6);
//		job2.waitForCompletion(true);
//
//		Configuration conf3 = new Configuration();
//
//		Job job3 = new Job(conf3, "step3");
//		job3.setJarByClass(CFRecommenderDriver.class);
//
//		job3.setMapperClass(UserVectorSplitterMapper.class);
//		job3.setReducerClass(Reducer.class);
//
//		job3.setMapOutputKeyClass(IntWritable.class);
//		job3.setMapOutputValueClass(VectorOrPrefWritable.class);
//
//		job3.setOutputKeyClass(IntWritable.class);
//		job3.setOutputValueClass(VectorOrPrefWritable.class);
//
//		FileInputFormat.addInputPath(job3, new Path("/CFRecommender/tmp/step15/step1"));
//		Path outPath3 = new Path("/CFRecommender/tmp/step23/step3");
//		FileOutputFormat.setOutputPath(job3, outPath3);
//		HadoopUtil.delete(conf3, new Path[] { outPath3 });
//		job3.setInputFormatClass(SequenceFileInputFormat.class);
//		job3.setOutputFormatClass(SequenceFileOutputFormat.class);
//		 job3.setNumReduceTasks(6);
//		job3.waitForCompletion(true);
//
//		Configuration conf4 = new Configuration();
//
//		Job job4 = new Job(conf4, "step4");
//		job4.setJarByClass(CFRecommenderDriver.class);
//
//		job4.setMapperClass(Mapper.class);
//		job4.setReducerClass(ToVectorAndPrefReducer.class);
//
//		job4.setMapOutputKeyClass(IntWritable.class);
//		job4.setMapOutputValueClass(VectorOrPrefWritable.class);
//
//		job4.setOutputKeyClass(IntWritable.class);
//		job4.setOutputValueClass(VectorAndPrefsWritable.class);
//
//		FileInputFormat.addInputPath(job4, new Path("/CFRecommender/tmp/step23/step*/"));
//		Path outPath4 = new Path("/CFRecommender/tmp/step4");
//		FileOutputFormat.setOutputPath(job4, outPath4);
//		HadoopUtil.delete(conf4, new Path[] { outPath4 });
//
//		job4.setInputFormatClass(SequenceFileInputFormat.class);
//		job4.setOutputFormatClass(SequenceFileOutputFormat.class);
//
//		 job4.setNumReduceTasks(6);
//		job4.waitForCompletion(true);
//
//		Configuration conf5 = new Configuration();
//
//		Job job5 = new Job(conf5, "step5");
//		job5.setJarByClass(CFRecommenderDriver.class);
//
//		job5.setMapperClass(PartialMultiplyMapper.class);
//		job5.setReducerClass(AggregateAndRecommendReducer.class);
//		job5.setCombinerClass(AggregateAndRecommendReducer.class);
//
//		job5.setMapOutputKeyClass(VLongWritable.class);
//		job5.setMapOutputValueClass(VectorWritable.class);
//
//		job5.setOutputKeyClass(VLongWritable.class);
//		job5.setOutputValueClass(VectorWritable.class);
//
//		FileInputFormat.addInputPath(job5, new Path("/CFRecommender/tmp/step4"));
//		Path outPath5 = new Path("/CFRecommender/tmp/step15/step5");
//		FileOutputFormat.setOutputPath(job5, outPath5);
//		HadoopUtil.delete(conf4, new Path[] { outPath5 });
//
//		job5.setInputFormatClass(SequenceFileInputFormat.class);
//		job5.setOutputFormatClass(SequenceFileOutputFormat.class);
//
//		 job5.setNumReduceTasks(6);
//		job5.waitForCompletion(true);
//
//		Configuration conf6 = new Configuration();
//
//		Job job6 = new Job(conf6, "step6");
//		job6.setJarByClass(CFRecommenderDriver.class);
//
//		job6.setMapperClass(Mapper.class);
//		job6.setReducerClass(ItemToRecommenderReducer.class);
//
//		job6.setMapOutputKeyClass(VLongWritable.class);
//		job6.setMapOutputValueClass(VectorWritable.class);
//
//		job6.setOutputKeyClass(VLongWritable.class);
//		job6.setOutputValueClass(RecommendedItemsWritable.class);
//
//		FileInputFormat.addInputPath(job6, new Path("/CFRecommender/tmp/step15/step*/"));
//		Path outPath6 = new Path(CFRecommenderJob.OutPutPath);
//		FileOutputFormat.setOutputPath(job6, outPath6);
//		HadoopUtil.delete(conf6, new Path[] { outPath6 });
//		job6.setInputFormatClass(SequenceFileInputFormat.class);
//		job6.setOutputFormatClass(TextOutputFormat.class);
//		job6.setNumReduceTasks(6);
//		job6.waitForCompletion(true);

		return 0;
	}

}
