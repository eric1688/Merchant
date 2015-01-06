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
 * 默认mapper，以2、3为输入，根据index归并信息reducer根据ShopID，把用户喜好信息、与其他店铺相似信息收集在一起。
 * 从VectorOrPrefWritable得到list（user）、list（preference）、商铺共同出现次数（vector)，
 * 根据index整合到一起得到VectorAndPrefWritable
 *
 *输出：（ ShopID，VectorAndPrefWritable ），
 *VectorAndPrefWritable 为{ Vector；list（user）；list（preference）} 中存储用户喜好信息、与其他店铺相似信息，list（user）；list（preference）一一对应。
 *如590 / { [22：1.0，95：2.0，...，9059：3.0，...] ；[ 98955，98966，...]；[3.0，4.0，...] }
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
