package org.apache.hama.computemodel.mapreduce;

import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.PriorityQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.bsp.Superstep;
import org.apache.hama.computemodel.mapreduce.Mapper.CombinerOutputCollector;

public class Reducer<K2 extends Writable, V2 extends Writable, K3 extends Writable, V3 extends Writable> 
extends Superstep<K2, V2, K3, V3, WritableKeyValues<K2, V2>> {
	
	public static final String REDUCER_CLASS = "hama.mapreduce.reducer";

	private PriorityQueue<WritableKeyValues<K2, V2>> memoryQueue;

	@Override
	protected void compute(
			BSPPeer<K2, V2, K3, V3, WritableKeyValues<K2, V2>> peer)
			throws IOException {
		
		this.memoryQueue = (PriorityQueue<WritableKeyValues<K2, V2>>) 
				peer.getSavedObject(Mapper.MESSAGE_QUEUE);
		
		Configuration conf = peer.getConfiguration();
		
		WritableKeyValues<K2, V2> message;
		while((message = peer.getCurrentMessage()) != null){
			this.memoryQueue.add(message);
		}
		
		CombinerOutputCollector<K2, V2> outputCollector = 
				new CombinerOutputCollector<K2, V2>();
		Comparator<V2> valComparator = null;
	    Class<?> comparatorClass = 
	    		conf.getClass(Mapper.VALUE_COMPARATOR_CLASS, null);
	    if (comparatorClass != null) {
	      valComparator = (Comparator<V2>) ReflectionUtils.newInstance(
	          comparatorClass, conf);
	    }

	    	    Iterator<WritableKeyValues<K2, V2>> recordIterator = memoryQueue.iterator();

	      CombinerOutputCollector<K2, V2> collector 
	      					= new CombinerOutputCollector<K2, V2>();

	      WritableKeyValues<K2, V2> previousRecord = null;

	      while (recordIterator.hasNext()) {

	        WritableKeyValues<K2, V2> record = recordIterator.next();
	        K2 key = record.getKey();
	        if (previousRecord != null 
	            && key.equals(previousRecord.getKey())) {
	          previousRecord.addValues(record.getValues());
	          recordIterator.remove();
	        } else {
	          if (previousRecord != null) {
	            previousRecord.sortValues(valComparator);
	          }
	          previousRecord = record;
	        }
	      }

	      org.apache.hadoop.mapred.Reducer<K2, V2, K2, V2> reducer = null;
		  Class<?> reducerClass = conf.getClass(REDUCER_CLASS, null);

		    if (reducerClass != null) {
		    	reducer = (org.apache.hadoop.mapred.Reducer<K2, V2, K2, V2>) 
		    		  ReflectionUtils.newInstance(reducerClass, conf);
		 }
		    
		 if(reducer != null){
			 Iterator<WritableKeyValues<K2, V2>> recordIter = 
					 this.memoryQueue.iterator();
			 while(recordIter.hasNext()){
				 WritableKeyValues<K2,V2> record = recordIter.next();
				 Iterator<V2> valIterator = record.getValues().iterator();
				 reducer.reduce(record.getKey(), valIterator, output, null);
				 
			 }
		 }
			

		
		
	}
	
	
	
	
	

}
