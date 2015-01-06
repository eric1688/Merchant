package org.bai.hadoop.step0;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

public class ValueComparator extends WritableComparator {
	private static final Text.Comparator TEXT_COMPARATOR=new Text.Comparator();
	private static final LongWritable.Comparator LONGWRITABLE_COMPARATOR=new LongWritable.Comparator();
	protected ValueComparator(){
		super(TextPair.class,true);
		
	}
	public int compare(WritableComparable w1,WritableComparable w2){
		TextPair ip1=(TextPair) w1;
		TextPair ip2=(TextPair) w2;
		//int cmp= TextPair.compare(ip1.getFirst(),ip2.getFirst());
		int cmp=ip1.compareTo(ip2);
		return cmp;
	}
	public int compare(byte[] b1,int s1,int l1,byte[] b2,int s2,int l2){
		try{
			int firstL1=WritableUtils.decodeVIntSize(b1[s1])+readVInt(b1,s1);
			int firstL2=WritableUtils.decodeVIntSize(b2[s2])+readVInt(b2,s2);
			int cmp=LONGWRITABLE_COMPARATOR.compare(b1, s1+firstL1,l1-firstL1,b2,s2+firstL2,l2-firstL2);
			if(cmp!=0){
				return cmp;
			}
			return TEXT_COMPARATOR.compare(b1, s1,firstL1,b2,s2,firstL2);
		}catch(Exception e){
			throw new IllegalArgumentException(e);
		}
	}
}
