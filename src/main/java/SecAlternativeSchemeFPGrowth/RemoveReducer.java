package SecAlternativeSchemeFPGrowth;

import java.util.Dictionary;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.vectorizer.DictionaryVectorizer;
/*
 * 
 * i 8 recourd g	f	c
i 7 recourd g	c	i	f
i 7 recourd g	c	k	i	f

7	f	b
7	g	b
7	c	e
7	g	c	e
7	g	c	i	e
7	g	c	k	i	e
7	g	k	c	e
7	c	i	e
7	c	k	i	e
7	k	c	e
8	f	c
8	g	c	f
7	g	c	i	f
7	g	c	k	i	f
8	g	c	k	f
7	c	i	f
7	c	k	i	f
8	c	k	f
9	g	c
7	g	c	h
7	g	c	i	h
7	g	c	k	i	h
7	g	c	k	h
8	g	c	i
8	g	c	k	i
7	g	c	k
7	c	h
7	c	i	h
7	c	k	i	h
7	c	k	h
8	c	i
8	c	k	i
9	k	c
8	g	e
7	g	i	e
7	g	k	i	e
8	g	k	e
7	i	e
7	k	i	e
8	k	e
8	g	f
7	g	i	f
7	g	k	i	f
8	g	k	f
7	i	f
7	k	i	f
8	k	f
7	g	h
7	g	i	h
7	g	k	i	h
7	g	k	h
9	g	i
8	g	k	i
7	g	j
10	g	k
7	i	h
7	k	i	h
7	k	h
8	k	i

 */
public class RemoveReducer extends Reducer<IntWritable,Record,Record,IntWritable>{
	//private DictionaryVectorizer;
	//private Dictionary<Int, String> d=new 
	private List<String> lld=new LinkedList<String>();
	public HashMap<IntWritable,List<Record>> hs=new HashMap<IntWritable,List<Record>>();
	public void reduce(IntWritable key,Iterable<Record> values,Context context){
		
		while (values.iterator().hasNext()) {
			Record record=values.iterator().next();
			int tmp=record.getList().size();
			//if()
		}
	}
	
}
