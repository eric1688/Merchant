package CFRecommendation.DataProcessing.step6;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.cf.taste.common.TopK;
import org.apache.mahout.cf.taste.hadoop.RecommendedItemsWritable;
import org.apache.mahout.cf.taste.impl.common.FastIDSet;
import org.apache.mahout.cf.taste.impl.recommender.GenericRecommendedItem;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.common.iterator.FileLineIterable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import CFRecommendation.CFRecommenderJob;
import Recommendation.HDFS.Util;


/*
 * 默认的mapper，step1、step5结果作为输出。reducer在setup中根据step0输出构建用于index和itemID转化的HashMap。
 * 在reduce函数中，先去掉值为0的元素得到迭代器，统计个数，并再次得到Vector。
 * 比较个数即可分别preferenceVector、 recommendationVector。
 * 把preferenceVector去0后做循环，把两者相同的元素在recommendationVector 中的值设为0。
 * 再根据recommendationVector得到对于用户前K个推荐度最高的index，做转化后写出到输出文件（recommender/output/）输出：（userID，Vector），
 * 如 98955 / [23：115.0，195：111.0，...]
 */
import com.google.common.primitives.Floats;

public class ItemToRecommenderReducer
		extends
		Reducer<VLongWritable, VectorWritable, VLongWritable, RecommendedItemsWritable> {

	// public static final String ITEMID_INDEX_PATH = "itemIDindexpath";
	public static final String NUM_RECOMMENDATIONS = "numRecommendations";
	public static final String ITEMS_FILE = "itemsFile";

	private static HashMap<Integer, Long> indextoIDMap = new HashMap<Integer, Long>();

	// private boolean booleanData;
	private int recommendationsPerUser;
	// TODO： 要推荐的items的集合,现在无用！要在setup中添加get方法
	private FastIDSet itemsToRecommendFor;

	@Override
	protected void setup(Context context) throws IOException {

		Configuration conf = context.getConfiguration();

		recommendationsPerUser = conf.getInt(NUM_RECOMMENDATIONS,
				CFRecommenderJob.DEFAULT_NUM_RECOMMENDATIONS);

		FileSystem fs = FileSystem.get(URI.create(Util.HDFS),
				conf);
		// "/recommender/index/part-r-00000"
		String dst = CFRecommenderJob.indexFilePath;
		FSDataInputStream hdfsInStream = fs.open(new Path(dst));
		BufferedReader reader = new BufferedReader(new InputStreamReader(
				hdfsInStream));

		String tmpString = null;
		while ((tmpString = reader.readLine()) != null) {
			String[] Pair = tmpString.split("\t");
			indextoIDMap.put(Integer.valueOf(Pair[0]), Long.parseLong(Pair[1]));
		}

		String itemFilePathString = conf.get(ITEMS_FILE);
		FSDataInputStream in = null;
		if (itemFilePathString == null) {
			itemsToRecommendFor = null;
		} else {
			Path unqualifiedItemsFilePath = new Path(itemFilePathString);
			FileSystem fst = FileSystem.get(unqualifiedItemsFilePath.toUri(),
					conf);
			itemsToRecommendFor = new FastIDSet();
			Path itemsFilePath = unqualifiedItemsFilePath.makeQualified(fst);
			in = fst.open(itemsFilePath);
			for (String line : new FileLineIterable(in)) {
				itemsToRecommendFor.add(Long.parseLong(line));
			}
		}
	}

	@Override
	public void reduce(VLongWritable userID, Iterable<VectorWritable> values,
			Context context) throws IOException, InterruptedException {

		int i = 0;
		Vector tmpVector0 = null;
		Vector tmpVector1 = null;
		int num0 = 0;
		int num1 = 0;
		for (VectorWritable value : values) {
			if (i == 0) {
				tmpVector0 = value.get();

			} else if (i == 1) {
				tmpVector1 = value.get();

			} else
				return;
			i++;
		}

		Iterator<Vector.Element> TestIterator0 = tmpVector0.iterateNonZero();
		if (tmpVector1 == null)
			return;
		Iterator<Vector.Element> TestIterator1 = tmpVector1.iterateNonZero();

		while (TestIterator0.hasNext()) {
			TestIterator0.next();
			num0++;
		}

		while (TestIterator1.hasNext()) {
			TestIterator1.next();
			num1++;
		}

		if (num0 > num1) {
			RemovetheSameItem(tmpVector0, tmpVector1, userID, context);
		} else {
			RemovetheSameItem(tmpVector1, tmpVector0, userID, context);
		}
	}

	private void RemovetheSameItem(Vector recommendationVector,
			Vector prefrenceVector, VLongWritable userID, Context context)
			throws IOException, InterruptedException {

		Iterator<Vector.Element> prefrenceIterator = prefrenceVector
				.iterateNonZero();

		while (prefrenceIterator.hasNext()) {
			recommendationVector
					.setQuick(prefrenceIterator.next().index(), 0.0);
		}

		writeRecommendedItems(userID, recommendationVector, context);
	}

	// Comparator排序
	private static final Comparator<RecommendedItem> BY_PREFERENCE_VALUE = new Comparator<RecommendedItem>() {
		public int compare(RecommendedItem one, RecommendedItem two) {
			return Floats.compare(one.getValue(), two.getValue());
		}
	};

	/**
	 * find the top entries in recommendationVector, map them to the real
	 * itemIDs and write back the result
	 */
	private void writeRecommendedItems(VLongWritable userID,
			Vector recommendationVector, Context context) throws IOException,
			InterruptedException {
		TopK<RecommendedItem> topKItems = new TopK<RecommendedItem>(
				recommendationsPerUser, BY_PREFERENCE_VALUE);

		Iterator<Vector.Element> recommendationVectorIterator = recommendationVector
				.iterateNonZero();
		while (recommendationVectorIterator.hasNext()) {
			Vector.Element element = recommendationVectorIterator.next();
			int index = element.index();
			long itemID;

			itemID = indextoIDMap.get(index);

			if (itemsToRecommendFor == null
					|| itemsToRecommendFor.contains(itemID)) {
				float value = (float) element.get();
				if (!Float.isNaN(value)) {
					topKItems.offer(new GenericRecommendedItem(itemID, value));
				}
			}
		}
		/*
		 * 
		 * RecommendedItemsWritable.toString(); public String toString() {
		 * StringBuilder result = new StringBuilder(200); result.append('[');
		 * boolean first = true; for (RecommendedItem item : this.recommended) {
		 * if (first) first = false; else { result.append(','); }
		 * result.append(String.valueOf(item.getItemID())); result.append(':');
		 * result.append(String.valueOf(item.getValue())); } result.append(']');
		 * return result.toString(); }
		 */
		if (!topKItems.isEmpty()) {
			context.write(userID,
					new RecommendedItemsWritable(topKItems.retrieve()));
		}
	}
}
