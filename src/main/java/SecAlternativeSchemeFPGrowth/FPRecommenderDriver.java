package SecAlternativeSchemeFPGrowth;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.mahout.math.VectorWritable;

import CFRecommendation.CFRecommenderDriver;
import CFRecommendation.Util.CFRecommendationUtil;
import Recommendation.HDFS.Util;
import Test.SourcedataToUSPrefMapper;
import Test.ToUserVectorReducer;
/*
 * 
 * 7	b	f
7	b	g
7	c	e
7	g	c	e
7	g	c	i	e
7	g	c	k	i	e
7	g	k	c	e
7	c	i	e
7	c	k	i	e
7	k	c	e
8	c	f
8	g	c	f
7	g	c	i	f
7	g	c	k	i	f
8	g	c	k	f
7	c	i	f
7	c	k	i	f
8	c	k	f
9	g	c
7	g	c	h
7	g	c	i	h
7	g	c	k	i	h
7	g	c	k	h
8	g	c	i
8	g	c	k	i
9	g	k	c
7	c	h
7	c	i	h
7	c	k	i	h
7	c	k	h
8	c	i
8	c	k	i
9	k	c
8	g	e
7	g	i	e
7	g	k	i	e
8	g	k	e
7	i	e
7	k	i	e
8	k	e
8	g	f
7	g	i	f
7	g	k	i	f
8	g	k	f
7	i	f
7	k	i	f
8	k	f
7	g	h
7	g	i	h
7	g	k	i	h
7	g	k	h
9	g	i
8	g	k	i
7	j	g
10	g	k
7	i	h
7	k	i	h
7	k	h
8	k	i

 */
public class FPRecommenderDriver extends Configured implements Tool {
	public int run(String[] args) throws Exception {

		Configuration conf0 = new Configuration();

		Job job0 = new Job(conf0, "step0");
		job0.setJarByClass(FPRecommenderDriver.class);
		job0.setMapperClass(ItemCountMapper.class);
		job0.setReducerClass(ItemCountReducer.class);

		job0.setMapOutputKeyClass(Text.class);
		job0.setMapOutputValueClass(IntWritable.class);
		job0.setOutputKeyClass(LongWritable.class);
		job0.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job0, new Path(Util.HDFS
				+ "/fprecommender/input/data"));
		FileOutputFormat.setOutputPath(job0, new Path(Util.HDFS
				+ "/fprecommender/input/tmp"));

		job0.setNumReduceTasks(1);
		job0.setOutputFormatClass(SequenceFileOutputFormat.class);
		job0.waitForCompletion(true);

		Job sortJob = new Job(conf0, "sort");
		sortJob.setJarByClass(FPRecommenderDriver.class);
		FileInputFormat.addInputPath(sortJob, new Path(Util.HDFS
				+ "/fprecommender/input/tmp"));
		sortJob.setInputFormatClass(SequenceFileInputFormat.class);

		/* 将 Reducer 的个数限定为1, 最终输出的结果文件就是一个。 */
		sortJob.setNumReduceTasks(1);
		FileOutputFormat.setOutputPath(sortJob, new Path(Util.HDFS
				+ "/fprecommender/input/F1"));

		sortJob.setOutputKeyClass(LongWritable.class);
		sortJob.setOutputValueClass(Text.class);
		/*
		 * Hadoop 默认对 IntWritable 按升序排序，而我们需要的是按降序排列。 因此我们实现了一个
		 * IntWritableDecreasingComparator 类,　 并指定使用这个自定义的 Comparator 类对输出结果中的
		 * key (词频)进行排序
		 */
		sortJob.setSortComparatorClass(LongKeyAscComparator.class);
		FileSystem fs = FileSystem.get(URI.create(Util.HDFS),
				conf0);
		fs.deleteOnExit(new Path(Util.HDFS + "/fprecommender/input/tmp"));
		sortJob.waitForCompletion(true);

		
		Configuration conf1 = new Configuration();

		Job job1 = new Job(conf1, "step1");
		job1.setJarByClass(CFRecommenderDriver.class);

		job1.setMapperClass(GroupMapper.class);
		job1.setReducerClass(FPReducer.class);
		// job1.setReducerClass(Reducer.class);

		job1.setMapOutputKeyClass(IntWritable.class);
		job1.setMapOutputValueClass(Record.class);

		job1.setOutputKeyClass(IntWritable.class);
		job1.setOutputValueClass(Text.class);

		//job1.setOutputFormatClass(SequenceFileOutputFormat.class);

		FileInputFormat.addInputPath(job1, new Path(Util.HDFS
				+ "/fprecommender/input/data"));
		FileOutputFormat.setOutputPath(job1, new Path(Util.HDFS
				+ "/fprecommender/output"));
		// job1.setNumReduceTasks(6);
		job1.setInputFormatClass(TextInputFormat.class);
		job1.setOutputFormatClass(TextOutputFormat.class);
		job1.waitForCompletion(true);

		
		Configuration conf2 = new Configuration();

		Job job2 = new Job(conf2, "step2");
		job2.setJarByClass(CFRecommenderDriver.class);

		job2.setMapperClass(InverseMapper.class);
		job2.setReducerClass(MaxReducer.class);
		// job1.setReducerClass(Reducer.class);

		job2.setMapOutputKeyClass(Record.class);
		job2.setMapOutputValueClass(IntWritable.class);

		job2.setOutputKeyClass(IntWritable.class);
		job2.setOutputValueClass(Record.class);

		//job2.setOutputFormatClass(SequenceFileOutputFormat.class);

		FileInputFormat.addInputPath(job2, new Path(Util.HDFS
				+ "/fprecommender/output/part-r-*"));
		FileOutputFormat.setOutputPath(job2, new Path(Util.HDFS
				+ "/fprecommender/output2"));
		// job1.setNumReduceTasks(6);
		//job2.setNumReduceTasks(0);
		job2.setInputFormatClass(TextInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);
		job2.waitForCompletion(true);
		
		return 0;

	}
}
