package CFRecommendation.DataProcessing.step2;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.cf.taste.hadoop.item.VectorOrPrefWritable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.RandomAccessSparseVector;

import Recommendation.HDFS.Util;

/*
reducer累加得到最终的item1和其他item共同出现次数（作为和其他店铺的相关度）
输出：（ShopID1，VectorOrPrefWritable），VectorOrPrefWritable 中存放其他店铺和该店铺的相关度。
如590/[22：1.0，95：2.0，...，9059：3.0，...] 
 */

public class UserVectorToCooccurrenceReducer extends
		Reducer<IntWritable, IntWritable, IntWritable, VectorOrPrefWritable> {
	private int numOfItem = 0;

	@Override
	protected void setup(Context context) throws IOException {

		Configuration conf = context.getConfiguration();
		FileSystem fs = FileSystem.get(URI.create(Util.HDFS),
				conf);

		FSDataInputStream hdfsInStream = fs.open(new Path(
				"/CFRecommender/tmp/index/numOfItem"));
		BufferedReader reader2 = new BufferedReader(new InputStreamReader(
				hdfsInStream));

		String numOfItemString = reader2.readLine();
		numOfItem = Integer.parseInt(numOfItemString);
	}

	@Override
	public void reduce(IntWritable itemIndex1,
			Iterable<IntWritable> itemIndex2s, Context context)
			throws IOException, InterruptedException {

		Vector cooccurrenceRow = new RandomAccessSparseVector(numOfItem, 100);

		for (IntWritable value : itemIndex2s) {
			int itemIndex2 = value.get();
			cooccurrenceRow.set(itemIndex2,
					cooccurrenceRow.get(itemIndex2) + 1.0);
		}
		context.write(itemIndex1, new VectorOrPrefWritable(cooccurrenceRow));
	}
}
