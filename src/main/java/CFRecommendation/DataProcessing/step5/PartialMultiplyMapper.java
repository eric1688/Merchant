package CFRecommendation.DataProcessing.step5;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.math.Vector;
import org.apache.hadoop.io.VLongWritable;
import org.apache.mahout.cf.taste.hadoop.item.VectorAndPrefsWritable;
/*
 * ����4�Ľ���ļ���Ϊ���룬��ˡ��ۼӵõ��ܵ��Ƽ��ȣ��õ��Ƽ�����mapper�����û����֣�
 * ���û����ֺ͵������Ƴ̶���˵õ��Ƽ��� recommond= preference* vector ��
 * ���<user,preference*vector>��
 * ��ֵ���� [ index1 : recommond1 ,index2 : recommond2 ,index3 : recommond3 ,......] ��
 * �� 98955 / [22��3.0��95��6.0��...��9059��9.0��...] 
 */
public class PartialMultiplyMapper
		extends
		Mapper<IntWritable, VectorAndPrefsWritable, VLongWritable, VectorWritable> {
	@Override
	public void map(IntWritable key,
			VectorAndPrefsWritable vectorAndPrefsWritable, Context context)
			throws IOException, InterruptedException {

		Vector cooccurrenceColumn = vectorAndPrefsWritable.getVector();
		List<Long> userIDs = vectorAndPrefsWritable.getUserIDs();
		List<Float> preferenceValues = vectorAndPrefsWritable.getValues();

		for (int i = 0; i < userIDs.size(); i++) {
			long userID = userIDs.get(i);
			float prefValue = preferenceValues.get(i);
			Vector partialProduct = cooccurrenceColumn.times(prefValue);

			context.write(new VLongWritable(userID), new VectorWritable(
					partialProduct));
		}
	}
}
