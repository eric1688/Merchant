package CFRecommendation.DataProcessing.step1;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.RandomAccessSparseVector;
import CFRecommendation.CFRecommenderJob;
import Recommendation.HDFS.Util;

/*
 reducer把相同的进行累加，得到用户去过每个商铺的次数，再根据次数、评分对应关系转化成每个用户对商铺的评分。
 输出：（userID，Vector），
Vector[index1: preference1, index2 : preference2, index3 : preference3, ...]
 中存放该用户对消费过的商铺的评分。如 98955 / [ 590: 3.0, 22: 4.0, 9059: 1.0 ]
 */

public class ToUserVectorReducer extends
		Reducer<VLongWritable, IntWritable, VLongWritable, VectorWritable> {

	private static HashMap<Integer, Double> timesToscoreMap = new HashMap<Integer, Double>();
	private int numOfItem = 0;

	@Override
	protected void setup(Context context) throws IOException {

		Configuration conf = context.getConfiguration();
		FileSystem fs = FileSystem.get(URI.create(Util.HDFS),
				conf);
		// "recommender/score/score";
		String dst = CFRecommenderJob.timesToscorePath;
		FSDataInputStream hdfsInStream = fs.open(new Path(dst));
		BufferedReader reader = new BufferedReader(new InputStreamReader(
				hdfsInStream));

		String tmpString = null;

		while ((tmpString = reader.readLine()) != null) {
			String[] Pair = tmpString.split("\t");

			timesToscoreMap.put(Integer.valueOf(Pair[0]),
					Double.parseDouble(Pair[1]));
		}

		FSDataInputStream hdfsInStream2 = fs.open(new Path(
				"/CFRecommender/tmp/index/numOfItem"));
		BufferedReader reader2 = new BufferedReader(new InputStreamReader(
				hdfsInStream2));

		String numOfItemString = reader2.readLine();
		numOfItem = Integer.parseInt(numOfItemString);

	}

	@Override
	public void reduce(VLongWritable userID, Iterable<IntWritable> itemPrefs,
			Context context) throws IOException, InterruptedException {

		Vector userVector = new RandomAccessSparseVector(numOfItem, 100);

		for (IntWritable value : itemPrefs) {
			int itemIDIndex = value.get();
			userVector.setQuick(itemIDIndex, userVector.get(itemIDIndex) + 1.0);
		}
		// userVector.iterateNonZero();多加了RandomacessSparseVector
		Iterator<Vector.Element> userIterator = ((RandomAccessSparseVector) userVector)
				.iterateNonZero();

		while (userIterator.hasNext()) {
			Vector.Element e = userIterator.next();
			int tmp = (int) e.get();
			if (timesToscoreMap.containsKey(tmp)) {
				userVector.setQuick(e.index(), timesToscoreMap.get(tmp));
			} else {
				while (!timesToscoreMap.containsKey(tmp) && tmp > 0) {
					tmp--;
				}
				if (tmp == 0) {
					userVector.setQuick(e.index(), 0.0);
					continue;
				}
				userVector.setQuick(e.index(), timesToscoreMap.get(tmp));
			}
		}

		context.write(userID, new VectorWritable(userVector));
	}
}
