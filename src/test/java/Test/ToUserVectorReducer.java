package Test;

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
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.RandomAccessSparseVector;
import CFRecommendation.CFRecommenderJob;

public class ToUserVectorReducer extends
		Reducer<VLongWritable, IntWritable, VLongWritable, VectorWritable> {

	private static HashMap<Integer, Double> timesToscoreMap = new HashMap<Integer, Double>();
	private int numOfItem = 0;

	@Override
	protected void setup(Context context) throws IOException {

		Configuration conf = context.getConfiguration();
		FileSystem fs = FileSystem.get(conf);
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
