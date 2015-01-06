package org.bai.hadoop.step0;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class TextPair implements WritableComparable<TextPair>{
	private Text first;
	private LongWritable second;
	public TextPair(){
		set(new Text(),new LongWritable());
	}
	public TextPair (Text key,LongWritable result){
		this.first=key;
		this.second=result;
	}
	
	public void set(Text first,LongWritable second){
		this.first=first;
		this.second=second;
	}
	public Text getFirst() {
		return first;
	}
	
	public LongWritable getSecond() {
		return second;
	}
	
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		first.readFields(in);
		second.readFields(in);
	}

	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		first.write(out);
		second.write(out);
	}

	public int compareTo(TextPair tp) {
		// TODO Auto-generated method stub
		int cmp=second.compareTo(tp.second);
		if(cmp!=0){
			return -cmp;
		}
		return first.compareTo(tp.first);
	}
//	public  int compareTo(LongWritable second2){
//		return second.compareTo(second2);
//	}
		public  String toString(){
		return first+"\t"+second;
	}
		public int hashCode(){
			return first.hashCode()*163+second.hashCode();
		}
		public boolean equals(Object o){
			if( o instanceof TextPair){
				TextPair tp=(TextPair) o;
				return first.equals(tp.first)&&second.equals(tp.second);
			}
			return false;
		}
}
