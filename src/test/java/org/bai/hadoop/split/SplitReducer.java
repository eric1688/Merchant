package org.bai.hadoop.split;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.RandomAccessSparseVector;

import CFRecommendation.CFRecommenderJob;

public class SplitReducer extends
		Reducer<LongWritable, Text,  Text,NullWritable> {

	
	public void reduce(LongWritable key, Text value,
			Context context) throws IOException, InterruptedException {

		System.out.println("key+"+key);
		System.out.println("value+"+value);

		context.write(value,null);
	}
}
