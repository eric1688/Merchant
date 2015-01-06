package org.bai.hadoop.split;

import java.io.IOException;
import java.util.regex.Pattern;




import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.mapreduce.Mapper;



public class SplitMapper extends
		Mapper<LongWritable, Text,NullWritable, Text> {

	

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
			
		System.out.println("key+"+key);
		System.out.println("value+"+value);
		
		context.write(null,value);
		}
	}

