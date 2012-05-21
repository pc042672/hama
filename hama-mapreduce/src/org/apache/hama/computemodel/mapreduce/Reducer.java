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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.bsp.Superstep;
import org.apache.hama.computemodel.mapreduce.Mapper.CombinerOutputCollector;

public abstract class Reducer<K2 extends WritableComparable<?>, V2 extends Writable, K3 extends WritableComparable<?>, V3 extends Writable>
    extends Superstep<K2, V2, K3, V3, WritableKeyValues<K2, V2>> {
  
  Log LOG = LogFactory.getLog(Reducer.class);

  public static final String REDUCER_CLASS = "hama.mapreduce.reducer";

  private PriorityQueue<WritableKeyValues<K2, V2>> memoryQueue;

  @Override
  protected void compute(BSPPeer<K2, V2, K3, V3, WritableKeyValues<K2, V2>> peer)
      throws IOException {

    this.memoryQueue = (PriorityQueue<WritableKeyValues<K2, V2>>) peer
        .getSavedObject(Mapper.MESSAGE_QUEUE);

    Configuration conf = peer.getConfiguration();

    WritableKeyValues<K2, V2> message;
    while ((message = peer.getCurrentMessage()) != null) {
      this.memoryQueue.add(message);
    }

    CombinerOutputCollector<K3, V3> outputCollector = 
        new CombinerOutputCollector<K3, V3>();
    Comparator<V2> valComparator = null;
    Class<?> comparatorClass = conf.getClass(Mapper.VALUE_COMPARATOR_CLASS,
        null);
    if (comparatorClass != null) {
      valComparator = (Comparator<V2>) ReflectionUtils.newInstance(
          comparatorClass, conf);
    }
    
    WritableKeyValues<K2, V2> previousRecord = null;
    
    List<WritableKeyValues<K2, V2>> list = 
        new ArrayList<WritableKeyValues<K2,V2>>(memoryQueue.size());

    while (!memoryQueue.isEmpty()) {

      WritableKeyValues<K2, V2> record = memoryQueue.poll();
      K2 key = record.getKey();
      if (previousRecord != null && key.equals(previousRecord.getKey())) {
        previousRecord.addValues(record.getValues());
      } else {
        if (previousRecord != null) {
          previousRecord.sortValues(valComparator);
          list.add(previousRecord);
        }
        previousRecord = record;
      }
    }

    Iterator<WritableKeyValues<K2, V2>> recordIter = list.iterator();
    while (recordIter.hasNext()) {
      WritableKeyValues<K2, V2> record = recordIter.next();
      Iterator<V2> valIterator = record.getValues().iterator();
      reduce(record.getKey(), valIterator, outputCollector);
    }

    LOG.debug("In reduder " + outputCollector
        .getCollectedRecords());
    
    Iterator<WritableKeyValues<K3, V3>> outputIter = outputCollector
        .getCollectedRecords().iterator();
    while (outputIter.hasNext()) {
      WritableKeyValues<K3, V3> output = outputIter.next();
      peer.write(output.getKey(), output.getValue());
    }
    
  }

  public abstract void reduce(K2 key, Iterator<V2> values,
      OutputCollector<K3, V3> output) throws IOException;
}
