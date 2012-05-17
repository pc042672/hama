/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
