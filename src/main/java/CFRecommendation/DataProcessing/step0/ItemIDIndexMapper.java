package CFRecommendation.DataProcessing.step0;

import java.io.IOException;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import Recommendation.HDFS.Util;

/*
 ��itemID��indexӳ���׼��������
 mapper����item��Ϣ�ļ������������õ�itemID�������α�ţ�
 д���������ļ���recommender/index/  Ŀ¼�£�����·����ָhdfs·������
 */
public class ItemIDIndexMapper extends
		Mapper<LongWritable, Text, IntWritable, VLongWritable> {
	private int numOfItem = 0;

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		// String[] tokens = CFRecommendationUtil.split(value.toString());
		String[] tokens = value.toString().split("\t");
		System.out.println("length " + tokens.length);
		if (tokens.length < 3) {
			return;
		}
		/*
		 * if (tokens[0].length() != 15) return; if (tokens.length < 50) return;
		 */
		long itemID = Long.parseLong(tokens[0]);
		context.write(new IntWritable(numOfItem++), new VLongWritable(itemID));
	}

	@Override
	protected void cleanup(Context context) throws IOException {
		Configuration config = context.getConfiguration();
		FileSystem hdfs = FileSystem.get(URI.create(Util.HDFS),
				config);

		FSDataOutputStream hdfsOutStream = hdfs.create(new Path(
				"/CFRecommender/tmp/index/numOfItem"));
		String numOfItemString = "" + numOfItem;
		hdfsOutStream.write(numOfItemString.getBytes());
		hdfsOutStream.close();
	}

}
