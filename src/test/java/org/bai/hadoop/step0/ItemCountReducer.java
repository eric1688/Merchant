package org.bai.hadoop.step0;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ItemCountReducer extends
		Reducer<Text, IntWritable, TextPair, NullWritable> {
	private static LongWritable result = new LongWritable();

	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		int sum = 0;
		for (IntWritable val : values) {
			sum += val.get();
		}
		result.set(sum);

		context.write(new TextPair(key,result),NullWritable.get());

	}

}
