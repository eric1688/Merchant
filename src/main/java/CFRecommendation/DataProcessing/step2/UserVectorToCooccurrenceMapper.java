package CFRecommendation.DataProcessing.step2;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.math.Vector;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.mapreduce.Mapper;


/*
 * step1�Ľ���ļ���Ϊ���룬�������̵���س̶ȣ�ͨ��mapper����step1�����
 * �������item��ͬ�����ڣ����<index1,index2>����590/22��590/9059
 */
public class UserVectorToCooccurrenceMapper extends
		Mapper<VLongWritable, VectorWritable, IntWritable, IntWritable> {

	@Override
	public void map(VLongWritable userID, VectorWritable userVector,
			Context context) throws IOException, InterruptedException {

		Iterator<Vector.Element> it = userVector.get().iterateNonZero();

		while (it.hasNext()) {
			int index1 = it.next().index();

			Iterator<Vector.Element> it2 = userVector.get().iterateNonZero();
			while (it2.hasNext()) {
				int index2 = it2.next().index();
				if (index1 != index2) {
					context.write(new IntWritable(index1), new IntWritable(
							index2));
				}
			}
		}
	}
}
