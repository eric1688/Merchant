package SecAlternativeSchemeFPGrowth;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;

import org.apache.hadoop.io.WritableComparable;

public class Record implements WritableComparable<Record> {

	private LinkedList<String> list;

	public Record() {
		setList(new LinkedList<String>());
	}

	public Record(String[] arr) {
		setList(new LinkedList<String>());
		for (int i = 0; i < arr.length; i++)
			getList().add(arr[i]);
	}

	@Override
	public String toString() {
		String str = getList().get(0);
		for (int i = 1; i < getList().size(); i++)
			str += "\t" + getList().get(i);
		return str;
	}

	public void readFields(DataInput in) throws IOException {
		getList().clear();
		String line = in.readUTF();
		String[] arr = line.split("\t");
		for (int i = 0; i < arr.length; i++)
			getList().add(arr[i]);
	}

	public void write(DataOutput out) throws IOException {
		out.writeUTF(this.toString());
	}

	public int compareTo(Record obj) {
		Collections.sort(getList());
		Collections.sort(obj.getList());
		return this.toString().compareTo(obj.toString());
	}
	public Boolean contain(Record obj){
		if(list.containsAll(obj.getList())){
			return true;
		}
		else{
			System.out.println("false");
			return false;
		}
				
	}
	public static void main (String[] args){
		String[] arr1={"g","k","i","f"};
		Record r1=new Record(arr1);
		String[] arr2={"i","f","g","m"};
		Record r2=new Record(arr2);
		if(r1.contain(r2)){
			System.out.println( r1.toString());
		}else{
			System.out.println(r2.toString());
		}
	}
	public LinkedList<String> getList() {
		return list;
	}

	public void setList(LinkedList<String> list) {
		this.list = list;
	}

}