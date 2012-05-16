package org.apache.hama.computemodel.mapreduce;

import java.util.Enumeration;

import org.apache.hadoop.conf.Configuration;

public abstract class ReducerKeyDesignator {
  
  public enum DesignateStrategy {
    MINIMIZE_COMMUNICATION,
    SINGLE_REDUCER,
    FIXED_REDUCER_COUNT
  }
  
  public enum KeyDistribution {
    CONTIGUOUS, NON_CONTIGUOUS
  }
  
  protected abstract void designateKeysToReducers(int[] keyDistribution, 
      final long[][] globalKeyDistribution, Configuration conf);
  
  public static ReducerKeyDesignator getReduceDesignator(
      DesignateStrategy strategy, KeyDistribution keyDistribution){
    
//    if(DesignateStrategy.MINIMIZE_COMMUNICATION.equals(strategy)){
//      
//    }
    
    //Assuming non contiguous and minimize communcation
    return new ReducerKeyDesignator(){

      @Override
      protected void designateKeysToReducers(int[] keyDistribution,
          long[][] globalKeyDistribution, Configuration conf) {
          for(int peerNumber = 0; 
              peerNumber < globalKeyDistribution.length; ++peerNumber){
            long max = 0L;
            int maxPartition = -1;
            for(int i = 0; i < globalKeyDistribution[0].length; ++i){
              if(max < globalKeyDistribution[peerNumber][i]){
                maxPartition = i;
                max = globalKeyDistribution[peerNumber][i];
              }
            }
            keyDistribution[peerNumber] = maxPartition;
          }
      }
      
    };
    
    
  }

}
