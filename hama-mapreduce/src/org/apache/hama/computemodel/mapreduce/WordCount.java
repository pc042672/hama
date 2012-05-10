package org.apache.hama.computemodel.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.bsp.OutputCollector;

public class WordCount {
	
	public static class WordCountMapper extends Mapper<LongWritable, Text, 
						Text, LongWritable>{

		@Override
		protected void map(LongWritable key, Text value,
				OutputCollector<Text, LongWritable> collector) {
			// TODO Auto-generated method stub
			
		}

		
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
