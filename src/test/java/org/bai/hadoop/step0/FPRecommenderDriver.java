package org.bai.hadoop.step0;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.mapred.lib.InverseMapper;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;

import Recommendation.HDFS.Util;


public class FPRecommenderDriver extends Configured implements Tool {
	public int run(String[] args) throws Exception {

		Configuration conf0 = new Configuration();

		Job job0 = new Job(conf0, "step0");
		job0.setJarByClass(FPRecommenderDriver.class);
		job0.setMapperClass(ItemCountMapper.class);
		job0.setReducerClass(ItemCountReducer.class);

		job0.setMapOutputKeyClass(Text.class);
		job0.setMapOutputValueClass(IntWritable.class);
		job0.setOutputKeyClass(TextPair.class);
		job0.setOutputValueClass(NullWritable.class);
		job0.setSortComparatorClass(ValueComparator.class);
		//job0.setGroupingComparatorClass(ValueComparator.class);
		FileInputFormat.addInputPath(job0, new Path(Util.HDFS
				+ "/user/orisun/input/data"));
		FileOutputFormat.setOutputPath(job0, new Path(Util.HDFS
				+ "/user/test"));

		//job0.setSortComparatorClass(ValueComparator.class);
		job0.setNumReduceTasks(1);
		
		job0.waitForCompletion(true);

		
	
		

		return 0;

	}
}
