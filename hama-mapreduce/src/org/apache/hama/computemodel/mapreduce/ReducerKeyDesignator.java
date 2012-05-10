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
    
    return new ReducerKeyDesignator(){

      @Override
      protected void designateKeysToReducers(int[] keyDistribution,
          long[][] globalKeyDistribution, Configuration conf) {
        
      }
      
    };
    
    
  }

}
