package CFRecommendation.DataProcessing.step3;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.cf.taste.hadoop.item.VectorOrPrefWritable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

/*
 * mapper以step1结果为输出，以index拆分， 输出<itemindex,VectorOrPrefWritable>，
 * VectorOrPrefWritable中存放用户对对商铺的偏好(user:preference)。如590 / [98955：3.0]
 */
public class UserVectorSplitterMapper
		extends
		Mapper<VLongWritable, VectorWritable, IntWritable, VectorOrPrefWritable> {

	@Override
	public void map(VLongWritable key, VectorWritable value, Context context)
			throws IOException, InterruptedException {

		long userID = key.get();
		Vector userVector = value.get();
		Iterator<Vector.Element> NonZeroIterator = userVector.iterateNonZero();

		while (NonZeroIterator.hasNext()) {
			Vector.Element e = NonZeroIterator.next();
			int itemIndex = e.index();
			float preferenceValue = (float) e.get();

			context.write(new IntWritable(itemIndex), new VectorOrPrefWritable(
					userID, preferenceValue));
		}
	}
}