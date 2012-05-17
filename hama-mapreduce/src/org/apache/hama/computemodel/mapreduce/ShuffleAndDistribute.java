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
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.PriorityQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.lib.HashPartitioner;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.bsp.Superstep;
import org.apache.hama.bsp.message.DiskQueue;
import org.apache.hama.bsp.sync.SyncException;
import org.apache.hama.computemodel.mapreduce.ReducerKeyDesignator.DesignateStrategy;
import org.apache.hama.computemodel.mapreduce.ReducerKeyDesignator.KeyDistribution;

public class ShuffleAndDistribute<K2 extends WritableComparable<?>, V2 extends Writable>
    extends
    Superstep<NullWritable, NullWritable, K2, V2, WritableKeyValues<? extends WritableComparable<?>, ? extends Writable>> {

  // private Map<Long, Long> keyDistributionMap = new HashMap<Long, Long>();
  private long[][] globalKeyDistribution;
  private PriorityQueue<WritableKeyValues<K2, V2>> memoryQueue;

  @Override
  protected void setup(
      BSPPeer<NullWritable, NullWritable, K2, V2, WritableKeyValues<? extends WritableComparable<?>, ? extends Writable>> peer) {
    // TODO Auto-generated method stub
    super.setup(peer);
    this.memoryQueue = (PriorityQueue<WritableKeyValues<K2, V2>>) peer
        .getSavedObject(Mapper.MESSAGE_QUEUE);

    this.globalKeyDistribution = (long[][]) peer
        .getSavedObject(Mapper.KEY_DIST);
  }

  protected void designateKeysToReducers(int[] keyDistribution,
      final long[][] globalKeyDistribution, Configuration conf) {
    Class<?> designatorClass = conf.getClass("", null);
    ReducerKeyDesignator designator = null;
    if (designatorClass == null) {
      designator = ReducerKeyDesignator.getReduceDesignator(
          DesignateStrategy.MINIMIZE_COMMUNICATION, KeyDistribution.CONTIGUOUS);
    } else {
      designator = (ReducerKeyDesignator) (ReflectionUtils.newInstance(
          designatorClass, conf));
    }

    designator.designateKeysToReducers(keyDistribution, globalKeyDistribution,
        conf);

  }

  @Override
  protected void compute(
      BSPPeer<NullWritable, NullWritable, K2, V2, WritableKeyValues<? extends WritableComparable<?>, ? extends Writable>> peer)
      throws IOException {
    int peerId = peer.getPeerId();
    Configuration conf = peer.getConfiguration();

    WritableKeyValues<WritableKeyValues<IntWritable, IntWritable>, LongWritable> message;
    while ((message = (WritableKeyValues<WritableKeyValues<IntWritable, IntWritable>, LongWritable>) peer
        .getCurrentMessage()) != null) {
      int peerNo = message.getKey().getKey().get();
      int partition = message.getKey().getValue().get();
      globalKeyDistribution[peerNo][partition] += message.getValue().get();
    }

    int[] keyDistribution = new int[globalKeyDistribution[0].length];

    designateKeysToReducers(keyDistribution, globalKeyDistribution, conf);

    int myKeyCount = 0;
    for (int i = 0; i < globalKeyDistribution[0].length; ++i) {
      myKeyCount += globalKeyDistribution[peerId][i];
    }

    PriorityQueue<WritableKeyValues<K2, V2>> mergeQueue = new PriorityQueue<WritableKeyValues<K2, V2>>(
        myKeyCount);
    Partitioner<K2, V2> partitioner = (Partitioner<K2, V2>) ReflectionUtils
        .newInstance(
            conf.getClass(Mapper.PARTITIONER_CLASS, HashPartitioner.class),
            conf);

    Iterator<WritableKeyValues<K2, V2>> keyValIter = this.memoryQueue
        .iterator();
    String[] peerNames = peer.getAllPeerNames();
    while (keyValIter.hasNext()) {
      WritableKeyValues<K2, V2> record = keyValIter.next();
      int partition = partitioner.getPartition(record.getKey(),
          record.getValue(), peer.getNumPeers()); // should be num reducers
                                                  // eventually
      int destPeerId = keyDistribution[partition];
      if (peerId != destPeerId) {
        peer.send(peerNames[destPeerId], record);
        keyValIter.remove();
      }
    }

//    try {
//      peer.sync();
//    } catch (SyncException e) {
//      // TODO Auto-generated catch block
//      e.printStackTrace();
//    } catch (InterruptedException e) {
//      // TODO Auto-generated catch block
//      e.printStackTrace();
//    }
  }

}
// K1 extends WritableComparable<K1>,V1,K2 extends WritableComparable<K2>,V2
// extends Writable, M extends WritableKeyValues<K2, V2>
