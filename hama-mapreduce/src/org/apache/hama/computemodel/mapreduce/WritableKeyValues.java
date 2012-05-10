package org.apache.hama.computemodel.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.serializer.WritableSerialization;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hama.bsp.Combiner;

public class WritableKeyValues<K,V> 
implements WritableComparable<WritableKeyValues<K,V>>{
	
	private K key;
	private V value;
	private List<V> valueList;
	private Comparator<V> valueComparator;
	
	
	
	
	
	private void initializeCollector(){
		collector = new CombinerOutputCollector<K, V>();
	}
	
	public WritableKeyValues(K key, V value){
		
//		WritableSerialization serialization = new WritableSerialization();
//	    if(serialization.accept(key.getClass()) 
//	    		  && serialization.accept(value.getClass())){
//	    	  serialization.getSerializer(key.getClass());
//	    	  
//	    }
//	    else throw new IllegalArgumentException("Check ");
		this.key = key;
		this.valueList = new ArrayList<V>();
		this.valueList.add(value);
		
	}
	
	public K getKey(){
	  return this.key;
	}
	
	public V getValue(){
       return this.valueList.get(0);
    }
	
	public List<V> getValues(){
		return this.valueList;
	}

	public void setKey(K key){
	  this.key = key;
	}
	
//	public void setValue(V value){
//	  this.value = value;
//	}
	
	public void addValue(V value){
		if(this.valueList == null){
			valueList = new ArrayList<V>();
		}
		valueList.add(value);
	}
	
	public void addValues(List<V> values){
		this.valueList.addAll(values);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		
		
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public int compareTo(WritableKeyValues<K, V> o) {
		// TODO Auto-generated method stub
		return 0;
	}
	
	public void combine(Reducer<K, V, K, V> combiner, 
			Comparator<V> valueComparator, 
			OutputCollector<K,V> collector) throws IOException{
		
		if(valueList == null)
			return;
		
		if(valueComparator != null){
			Collections.sort(this.valueList, valueComparator);
		}
		
		combiner.reduce(key, this.valueList.iterator(), collector , null);
	}
	
	public void sortValues(Comparator<V> valueComparator){
		Collections.sort(this.valueList, valueComparator);
	}

}
