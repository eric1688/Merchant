package SecAlternativeSchemeFPGrowth;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class InverseMapper extends
		Mapper<LongWritable, Text, Record, IntWritable> {
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] arr = value.toString().split("\t");
		int count = Integer.parseInt(arr[0]);
		System.out.println("int count "+ count);
		Record record = new Record();
		for (int i = 1; i < arr.length; i++) {
			record.getList().add(arr[i]);
		}
		System.out.println("record "+record.toString());
		context.write(record, new IntWritable(count));
	}
}
