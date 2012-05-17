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

import java.util.Comparator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hama.bsp.BSPJob;


public class MapRedConf{

  private Class<? extends Mapper> mapperClass;
  private Class<? extends Reducer> reducerClass;
  private Class<? extends Reducer> combinerClass;

  private Class<? extends Comparator<?>> valueComparatorClass;
  private Class<? extends Partitioner<?, ?>> partitionerClass;
  
  
  int numMapTasks;
  int numReduceTasks;

  public Class<? extends Mapper> getMapperClass() {
    return mapperClass;
  }
  public void setMapperClass(Class<? extends Mapper> mapperClass) {
    this.mapperClass = mapperClass;
  }
  public Class<? extends Reducer> getReducerClass() {
    return reducerClass;
  }
  public void setReducerClass(Class<? extends Reducer> reducerClass) {
    this.reducerClass = reducerClass;
  }
  public Class<? extends Reducer> getCombinerClass() {
    return combinerClass;
  }
  public void setCombinerClass(Class<? extends Reducer> combinerClass) {
    this.combinerClass = combinerClass;
  }
  public int getNumMapTasks() {
    return numMapTasks;
  }
  public void setNumMapTasks(int numMapTasks) {
    this.numMapTasks = numMapTasks;
  }
  public int getNumReduceTasks() {
    return numReduceTasks;
  }
  public void setNumReduceTasks(int numReduceTasks) {
    this.numReduceTasks = numReduceTasks;
  }
  
  
  

  public Class<? extends Comparator<?>> getValueComparatorClass() {
    return valueComparatorClass;
  }
  public void setValueComparatorClass(
      Class<? extends Comparator<?>> valueComparatorClass) {
    this.valueComparatorClass = valueComparatorClass;
  }
  public Class<? extends Partitioner<?, ?>> getPartitionerClass() {
    return partitionerClass;
  }
  public void setPartitionerClass(
      Class<? extends Partitioner<?, ?>> partitionerClass) {
    this.partitionerClass = partitionerClass;
  }
  public void configureBSPJob(BSPJob bspJob){

    bspJob.setSupersteps(mapperClass, ShuffleAndDistribute.class, reducerClass);
    
    if(this.combinerClass != null)
      bspJob.getConf().setClass(Mapper.COMBINER_CLASS, 
        this.combinerClass, Reducer.class);
    if(this.partitionerClass != null)
      bspJob.getConf().setClass(Mapper.PARTITIONER_CLASS,
        this.partitionerClass, Partitioner.class);
    if(this.valueComparatorClass != null)
      bspJob.getConf().setClass(Mapper.VALUE_COMPARATOR_CLASS, 
        valueComparatorClass, Comparator.class);
    
    
  }
  
  
}
