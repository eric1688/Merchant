package SecAlternativeSchemeFPGrowth;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class MaxReducer extends
		Reducer<Record, IntWritable, IntWritable, Record> {
	public void reduce(Record key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		int max = -1;
		for (IntWritable value : values) {
			int i = value.get();
			if (i > max)
				System.out.println("i "+i+" recourd "+key.toString());
			
				max = i;
		}
		context.write(new IntWritable(max), key);
	}
}