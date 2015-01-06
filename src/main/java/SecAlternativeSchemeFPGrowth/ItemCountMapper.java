package SecAlternativeSchemeFPGrowth;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ItemCountMapper extends
		Mapper<LongWritable, Text, Text, IntWritable> {
	private static IntWritable one = new IntWritable(1);
	private Text Item = new Text();

	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		StringTokenizer st = new StringTokenizer(value.toString(), ",");
		while (st.hasMoreTokens()) {
			Item.set(st.nextToken());
			context.write(Item, one);
		}
	}
}
