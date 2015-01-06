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
 * mapper��step1���Ϊ�������index��֣� ���<itemindex,VectorOrPrefWritable>��
 * VectorOrPrefWritable�д���û��Զ����̵�ƫ��(user:preference)����590 / [98955��3.0]
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