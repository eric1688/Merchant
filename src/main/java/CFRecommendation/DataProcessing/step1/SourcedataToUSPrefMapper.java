package CFRecommendation.DataProcessing.step1;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import CFRecommendation.CFRecommenderJob;
import Recommendation.HDFS.Util;

/*
 mapper首先在setup过程中，根据step0输出文件构建hash表（HashMap）。
 在map函数中对文件内容处理，把itemID转换成index，输出<userID,index>对。
 输出：（userID，index），如98955 / 590
 */
public class SourcedataToUSPrefMapper extends
		Mapper<LongWritable, Text, VLongWritable, IntWritable> {

	// public static final String ITEMID_INDEX_PATH = "itemIDindexpath";
	// private static final Pattern SEPARATOR = Pattern.compile("[\t,]");

	private static HashMap<Long, Integer> IDtoindexMap = new HashMap<Long, Integer>();

	@Override
	protected void setup(Context context) throws IOException {

		Configuration conf = context.getConfiguration();
		FileSystem fs = FileSystem.get(URI.create(Util.HDFS),
				conf);
		// "recommender/index/part-r-00000";
		String dst = CFRecommenderJob.indexFilePath;
		FSDataInputStream hdfsInStream = fs.open(new Path(dst));
		BufferedReader reader = new BufferedReader(new InputStreamReader(
				hdfsInStream));

		String tmpString = null;

		while ((tmpString = reader.readLine()) != null) {
			String[] Pair = tmpString.split("\t");
			if (Pair[0].equalsIgnoreCase(""))
				break;
			if (Pair[1].equalsIgnoreCase(""))
				break;
			IDtoindexMap.put(Long.parseLong(Pair[1]), Integer.valueOf(Pair[0]));
		}
	}

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		// String[] tokens = CFRecommendationUtil.split(value.toString());
		String[] tokens = value.toString().split("\t");
		System.out.println("tokens " + tokens + " length " + tokens.length);
		// if (!tokens[48].equals("S22") || tokens.length < 100)
		// return;
		if (tokens.length < 3) {
			return;
		}
		long userID = Long.parseLong(tokens[1].substring(4));
		long itemID = Long.parseLong(tokens[2]);

		if (IDtoindexMap.containsKey(itemID)) {
			int t = IDtoindexMap.get(itemID);
			context.write(new VLongWritable(userID), new IntWritable(t));
		}

	}

}
