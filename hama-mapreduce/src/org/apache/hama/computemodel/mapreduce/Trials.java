package org.apache.hama.computemodel.mapreduce;

import org.apache.hadoop.io.serializer.WritableSerialization;

public class Trials {

	public static class IntWrap{
		int suraj = 0;
	}
	
	public static void main(String[] args){
		
		String suraj = "AB";
		IntWrap wrap = new IntWrap();
		
		WritableSerialization serialization = new WritableSerialization();
		System.out.println(serialization.accept(suraj.getClass()));
		System.out.println(serialization.accept(wrap.getClass()));
		
		
	}
}
