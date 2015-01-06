package CFRecommendation.DataProcessing.step4;

import java.io.IOException;
import java.util.List;
import com.google.common.collect.Lists;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.cf.taste.hadoop.item.VectorAndPrefsWritable;
import org.apache.mahout.cf.taste.hadoop.item.VectorOrPrefWritable;
import org.apache.mahout.math.Vector;

/*
 * Ĭ��mapper����2��3Ϊ���룬����index�鲢��Ϣreducer����ShopID�����û�ϲ����Ϣ������������������Ϣ�ռ���һ��
 * ��VectorOrPrefWritable�õ�list��user����list��preference�������̹�ͬ���ִ�����vector)��
 * ����index���ϵ�һ��õ�VectorAndPrefWritable
 *
 *������� ShopID��VectorAndPrefWritable ����
 *VectorAndPrefWritable Ϊ{ Vector��list��user����list��preference��} �д洢�û�ϲ����Ϣ������������������Ϣ��list��user����list��preference��һһ��Ӧ��
 *��590 / { [22��1.0��95��2.0��...��9059��3.0��...] ��[ 98955��98966��...]��[3.0��4.0��...] }
 */
public final class ToVectorAndPrefReducer
		extends
		Reducer<IntWritable, VectorOrPrefWritable, IntWritable, VectorAndPrefsWritable> {

	@Override
	public void reduce(IntWritable key, Iterable<VectorOrPrefWritable> values,
			Context context) throws IOException, InterruptedException {

		List<Long> userIDs = Lists.newArrayList();
		List<Float> prefValues = Lists.newArrayList();
		Vector similarityMatrixColumn = null;
		for (VectorOrPrefWritable value : values) {
			if (value.getVector() == null) {
				// Then this is a user-pref value
				userIDs.add(value.getUserID());
				prefValues.add(value.getValue());
			} else {
				// Then this is the column vector
				if (similarityMatrixColumn != null) {
					throw new IllegalStateException(
							"Found two similarity-matrix columns for item index "
									+ key.get());
				}
				similarityMatrixColumn = value.getVector();
			}
		}

		if (similarityMatrixColumn == null) {
			return;
		}

		VectorAndPrefsWritable vectorAndPrefs = new VectorAndPrefsWritable(
				similarityMatrixColumn, userIDs, prefValues);
		context.write(key, vectorAndPrefs);
	}

}
