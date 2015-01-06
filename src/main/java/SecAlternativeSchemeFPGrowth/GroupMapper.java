package SecAlternativeSchemeFPGrowth;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.BitSet;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import CFRecommendation.Util.CFRecommendationUtil;
import Recommendation.HDFS.Util;

public class GroupMapper extends Mapper<LongWritable,Text,IntWritable,Record> {

	List<String> freq = new LinkedList<String>(); 
	List<List<String>> freq_group = new LinkedList<List<String>>(); 
	public void setup(Context context) throws IOException {
		// 从文件读入频繁1项集
		Configuration conf = context.getConfiguration();
		FileSystem fs = FileSystem.get(
				URI.create(Util.HDFS), conf);
		// 薯片:7鸡蛋:7面包:7牛奶:6啤酒:4
		Path freqFile = new Path(Util.HDFS
				+ "/fprecommender/input/F1/part-r-00000");
		FSDataInputStream in = fs.open(freqFile);
		InputStreamReader isr = new InputStreamReader(in);
		BufferedReader br = new BufferedReader(isr);
		try {
			String line;
			while ((line = br.readLine()) != null) {
				String[] str = line.split("\t");
				String word = str[1];
				int value=Integer.parseInt(str[0]);
				//if(value>FPRecommendationUtil.minSuport){
				//System.out.println("word+"+word);
				freq.add(word);
				//
			}
		} finally {
			br.close();
		}
//		for(int i=0; i<freq.size();i++){
//			System.out.println(freq.get(i).toString());
//		}
		// 对频繁1项集进行分组
		Collections.shuffle(freq); // 打乱顺序
		int GroupNum=FPRecommendationUtil.GroupNum;
		int cap = freq.size() /GroupNum; // 每段分为一组
		//System.out.println("cap "+cap);
		for (int i = 0; i < GroupNum; i++) {
			List<String> list = new LinkedList<String>();
			for (int j = 0; j < cap; j++) {
				list.add(freq.get(i * cap + j));
				//System.out.println("freq.get(i * cap + j) "+freq.get(i * cap + j)+" i "+i+" j "+j);
			}
			freq_group.add(list);
		}
		int remainder = freq.size() % GroupNum;
		int base = GroupNum * cap;
		//System.out.println("base "+base);
		for (int i = 0; i < remainder; i++) {
			//System.out.println(freq.get(base+i));
			freq_group.get(i).add(freq.get(base + i));
		}
	}
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] arr = value.toString().split(",");
		Record record = new Record(arr);
		LinkedList<String> list = record.getList();
		BitSet bs = new BitSet(freq_group.size());
		bs.clear();
		while (record.getList().size() > 0) {
			String item = list.peekLast(); // 取出record的最后一项
			//System.out.println("item "+ item);
			int i = 0;
			for (; i < freq_group.size(); i++) {
				if (bs.get(i))
					continue;
				if (freq_group.get(i).contains(item)) {
					bs.set(i);
					break;
				}
			}
			if (i < freq_group.size()) { // 找到了
				context.write(new IntWritable(i), record);
				System.out.println("i "+i+" 分组 "+record.toString());
			}
			// 返回list最后一个值
			
		}
		
	}
	
}

