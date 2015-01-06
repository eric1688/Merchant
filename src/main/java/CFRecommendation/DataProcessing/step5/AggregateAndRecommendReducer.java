package CFRecommendation.DataProcessing.step5;

import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import java.io.IOException;
/*
 * combiner\reducer根据user把 preference*vector叠加。
 */
public final class AggregateAndRecommendReducer extends
		Reducer<VLongWritable, VectorWritable, VLongWritable, VectorWritable> {

	@Override
	public void reduce(VLongWritable userID, Iterable<VectorWritable> values,
			Context context) throws IOException, InterruptedException {

		Vector predictionVector = null;
		for (VectorWritable value : values) {
			predictionVector = predictionVector == null ? value.get()
					: predictionVector.plus(value.get());
		}

		context.write(userID, new VectorWritable(predictionVector));

	}

}