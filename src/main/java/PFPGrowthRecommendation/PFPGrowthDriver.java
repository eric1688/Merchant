package PFPGrowthRecommendation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Comparator;

import org.apache.commons.io.output.NullWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.Parameters;
import org.apache.mahout.common.iterator.sequencefile.PathType;
import org.apache.mahout.fpm.pfpgrowth.TransactionTree;
import org.apache.mahout.fpm.pfpgrowth.convertors.string.TopKStringPatterns;
import org.apache.mahout.math.list.IntArrayList;

import com.google.common.collect.Lists;

import PFPGrowthRecommendation.Util.FPUtil;
import PFPGrowthRecommendation.step0.PreDatapreprocessingMapper;
import PFPGrowthRecommendation.step0.PreDatapreprocessingReducer;
import PFPGrowthRecommendation.step1.ParallelCountingMapper;
import PFPGrowthRecommendation.step1.ParallelCountingReducer;
import PFPGrowthRecommendation.step2.IndexSortGroupItems;
import PFPGrowthRecommendation.step3.ParallelFPGrowthCombiner;
import PFPGrowthRecommendation.step3.ParallelFPGrowthMapper;
import PFPGrowthRecommendation.step3.ParallelFPGrowthReducer;
import PFPGrowthRecommendation.step4.AggregatorMapper;
import PFPGrowthRecommendation.step4.AggregatorReducer;
import Recommendation.HDFS.Util;

/*
 * 
 * b	([b],8), ([g, b],7), ([f, b],7)
 c	([g, k, c],9), ([g, k, c, i],8), ([g, f, k, c],8), ([g, k, c, i, e],7), ([g, f, k, c, i],7)
 e	([g, k, e],8), ([g, k, c, i, e],7)
 f	([f],10), ([g, f, k, c],8), ([g, f, k],8), ([g, f, k, c, i],7), ([f, b],7)
 g	([g],11), ([g, k],10), ([g, k, c],9), ([g, i],9), ([g, k, c, i],8), ([g, f, k, c],8), ([g, k, e],8), ([g, f, k],8), ([g, k, c, i, e],7), ([g, f, k, c, i],7), ([g, j],7), ([g, b],7)
 i	([g, i],9), ([g, k, c, i],8), ([g, k, c, i, e],7), ([g, f, k, c, i],7)
 j	([j],8), ([g, j],7)
 k	([g, k],10), ([g, k, c],9), ([g, k, c, i],8), ([g, f, k, c],8), ([g, k, e],8), ([g, f, k],8), ([g, k, c, i, e],7), ([g, f, k, c, i],7)

 */
public class PFPGrowthDriver extends Configured implements Tool {
	public static int maxPerGroup;
	public int run(String[] args) throws Exception

	{
		Configuration conf = new Configuration();
		Predataprocessing(conf);
		startParallelCounting(conf);
//		startSortGroupIitems(conf);
//		startParallelFPGrowth(conf);
//		startAggregating(conf);
		return 0;
	}
	public static void Predataprocessing(Configuration conf)throws IOException,InterruptedException,ClassNotFoundException{
		String input=Util.HDFS+"/CFRecommender/tmp/step23/step2";
		Job job=new Job(conf,"Predata processing Driver running over input");
		job.setJarByClass(PFPGrowthDriver.class);
		
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setMapperClass(PreDatapreprocessingMapper.class);
		job.setReducerClass(PreDatapreprocessingReducer.class);
		
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(input));
		
		Path outPath = new Path(Util.HDFS+"/tmp/test/output");
		FileOutputFormat.setOutputPath(job, outPath);
		HadoopUtil.delete(conf, new Path[] { outPath });
		job.setOutputFormatClass(TextOutputFormat.class);
		
		boolean succeeded = job.waitForCompletion(true);
		if (!succeeded)
			throw new IllegalStateException("Job failed!");

	}
	public static void startParallelCounting(Configuration conf)
			throws IOException, InterruptedException, ClassNotFoundException {

		//String input = PFPGrowthJob.InputPath;
		String input=Util.HDFS+"/tmp/test/output";
		Job job = new Job(conf, "Parallel Counting Driver running over input");
		job.setJarByClass(PFPGrowthDriver.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		FileInputFormat.addInputPath(job, new Path(input));
		Path outPath = new Path(PFPGrowthJob.CountingOutPath);
		FileOutputFormat.setOutputPath(job, outPath);

		HadoopUtil.delete(conf, new Path[] { outPath });

		job.setInputFormatClass(TextInputFormat.class);
		job.setMapperClass(ParallelCountingMapper.class);
		job.setCombinerClass(ParallelCountingReducer.class);
		job.setReducerClass(ParallelCountingReducer.class);
		// job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		boolean succeeded = job.waitForCompletion(true);
		if (!succeeded)
			throw new IllegalStateException("Job failed!");
	}
	public static void  startSortGroupIitems(Configuration conf)throws IOException, InterruptedException, ClassNotFoundException {
		List fList = IndexSortGroupItems.readFList();

		IndexSortGroupItems.saveFList(fList, conf);

		int numGroups = PFPGrowthJob.numGroups;
		 maxPerGroup = fList.size() / numGroups;
		
		if (fList.size() % numGroups != 0)
			maxPerGroup++;
		fList = null;
	}
	
	public static void startParallelFPGrowth(Configuration conf)
			throws IOException, InterruptedException, ClassNotFoundException {

		Path input = new Path(PFPGrowthJob.InputPath);
		Job job = new Job(conf, "PFP Growth Driver running over input");
		job.setJarByClass(PFPGrowthDriver.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(TransactionTree.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(TopKStringPatterns.class);

		FileInputFormat.addInputPath(job, input);
		Path outPath = new Path(PFPGrowthJob.StartingGrowthOutPath);
		FileOutputFormat.setOutputPath(job, outPath);

		HadoopUtil.delete(conf, new Path[] { outPath });

		job.setInputFormatClass(TextInputFormat.class);
		job.setMapperClass(ParallelFPGrowthMapper.class);
		job.setCombinerClass(ParallelFPGrowthCombiner.class);
		job.setReducerClass(ParallelFPGrowthReducer.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		boolean succeeded = job.waitForCompletion(true);
		if (!succeeded)
			throw new IllegalStateException("Job failed!");
	}

	public static void startAggregating(Configuration conf) throws IOException,
			InterruptedException, ClassNotFoundException {

		Path input = new Path(PFPGrowthJob.StartingGrowthOutPath + "/part-r-*");
		Job job = new Job(conf, "PFP Aggregator Driver running over input"
				+ input);
		job.setJarByClass(PFPGrowthDriver.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(TopKStringPatterns.class);

		FileInputFormat.addInputPath(job, input);
		Path outPath = new Path(PFPGrowthJob.OutputPath);
		FileOutputFormat.setOutputPath(job, outPath);

		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setMapperClass(AggregatorMapper.class);
		job.setCombinerClass(AggregatorReducer.class);
		job.setReducerClass(AggregatorReducer.class);
		// job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		HadoopUtil.delete(conf, new Path[] { outPath });
		boolean succeeded = job.waitForCompletion(true);
		if (!succeeded)
			throw new IllegalStateException("Job failed!");
	}

	public static IntArrayList getGroupMembers(int groupId, int maxPerGroup,
			int numFeatures) {
		IntArrayList ret = new IntArrayList();
		int start = groupId * maxPerGroup;
		int end = start + maxPerGroup;
		if (end > numFeatures)
			end = numFeatures;
		for (int i = start; i < end; i++) {
			ret.add(i);
		}
		return ret;
	}
}
