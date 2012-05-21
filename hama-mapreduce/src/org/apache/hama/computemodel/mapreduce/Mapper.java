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
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.lib.HashPartitioner;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.bsp.OutputCollector;
import org.apache.hama.bsp.Superstep;
import org.apache.hama.util.KeyValuePair;

public abstract class Mapper<K1, V1, K2 extends WritableComparable<?>, V2 extends Writable>
    extends
    Superstep<K1, V1, K2, V2, WritableKeyValues<? extends WritableComparable<?>, ? extends Writable>> {

  public static final Log LOG = LogFactory.getFactory().getLog(Mapper.class);

  public static final String VALUE_COMPARATOR_CLASS = "hama.mapreduce.valuecompare";
  public static final String COMBINER_CLASS = "hama.mapreduce.combiner";
  public static final String PARTITIONER_CLASS = "hama.mapreduce.keypartitioner";
  public static final String MESSAGE_QUEUE = "MESSAGE_QUEUE";
  public static final String KEY_DIST = "KEY_DISTRIBUTION";
  public static final String COMBINER_FUTURE = "COMBINER_FUTURE";
  private long[][] globalKeyDistribution;
  private PriorityQueue<WritableKeyValues<K2, V2>> memoryQueue;

  private static class CombineAndSortThread<K2 extends WritableComparable<?>, V2 extends Writable>
      implements Callable<Integer> {

    PriorityQueue<WritableKeyValues<K2, V2>> queue;
    Comparator<V2> valueComparator;
    Reducer<K2, V2, K2, V2> combinerInstance;

    CombineAndSortThread(Configuration conf,
        PriorityQueue<WritableKeyValues<K2, V2>> messageQueue,
        Comparator<V2> valComparator, Reducer<K2, V2, K2, V2> combiner) {
      queue = messageQueue;
      valueComparator = valComparator;
      combinerInstance = combiner;
    }

    @Override
    public Integer call() throws Exception {
      // TODO Auto-generated method stub
      Iterator<WritableKeyValues<K2, V2>> recordIterator = queue.iterator();

      CombinerOutputCollector<K2, V2> collector = new CombinerOutputCollector<K2, V2>();

      WritableKeyValues<K2, V2> previousRecord = null;

      while (!queue.isEmpty()) {

        WritableKeyValues<K2, V2> record = queue.poll();
        K2 key = record.getKey();
        if (previousRecord != null && combinerInstance != null
            && key.equals(previousRecord.getKey())) {
          previousRecord.addValue(record.getValue());
        } else {
          if (previousRecord != null && combinerInstance != null) {
            previousRecord
                .combine(combinerInstance, valueComparator, collector);
          }
          previousRecord = record;
        }
      }
      
      
      queue.clear();
      queue.addAll(collector.getCollectedRecords());
      collector.reset();

      return queue.size();
    }

  }

  public static class BSPMapperOutputCollector<K1, V1, K2 extends WritableComparable<?>, V2 extends Writable>
      implements OutputCollector<K2, V2> {

    BSPPeer<K1, V1, K2, V2, WritableKeyValues<? extends WritableComparable<?>, ? extends Writable>> bspPeer;
    final int partitions;
    final Configuration job;
    PriorityQueue<WritableKeyValues<K2, V2>> collectorQueue;
    Partitioner<K2, V2> partitioner;
    Map<Integer, Long> keyDistributionMap;
    long[] keyDistribution;

    // SortedMessageQueue<ByteWritable> sortedQueue;

    public BSPMapperOutputCollector(
        BSPPeer<K1, V1, K2, V2, WritableKeyValues<? extends WritableComparable<?>, ? extends Writable>> peer,
        PriorityQueue<WritableKeyValues<K2, V2>> diskQueue,
        long[] peerKeyDistribution) {
      bspPeer = peer;
      this.job = peer.getConfiguration();
      this.partitions = peer.getNumPeers();
      this.collectorQueue = diskQueue;

      this.partitioner = (Partitioner<K2, V2>) ReflectionUtils.newInstance(
          job.getClass(PARTITIONER_CLASS, HashPartitioner.class), job);

      this.keyDistribution = peerKeyDistribution;
    }

    @Override
    public void collect(K2 key, V2 value) throws IOException {
      WritableKeyValues<K2, V2> keyValPair = new WritableKeyValues<K2, V2>(key,
          value);
      this.collectorQueue.add(keyValPair);

      int partition = this.partitioner.getPartition(key, value, partitions);

      if (partition >= 0 && partition < keyDistribution.length) {
        keyDistribution[partition] += 1;
      }
    }

  }

  public static class CombinerOutputCollector<K extends WritableComparable<?>, V extends Writable>
      implements org.apache.hadoop.mapred.OutputCollector<K, V> {

    private List<WritableKeyValues<K, V>> collectBuffer = new ArrayList<WritableKeyValues<K, V>>();

    public List<WritableKeyValues<K, V>> getCollectedRecords() {
      return this.collectBuffer;
    }

    @Override
    public void collect(K key, V value) throws IOException {
      collectBuffer.add(new WritableKeyValues<K, V>(key, value));
    }

    public void reset() {
      this.collectBuffer = new ArrayList<WritableKeyValues<K, V>>();
    }

  }

  @Override
  protected void compute(
      BSPPeer<K1, V1, K2, V2, WritableKeyValues<? extends WritableComparable<?>, ? extends Writable>> peer)
      throws IOException {

    this.memoryQueue = new PriorityQueue<WritableKeyValues<K2, V2>>();
    this.globalKeyDistribution = new long[peer.getNumPeers()][peer.getNumPeers()];
    
    int myId = peer.getPeerId();
    OutputCollector<K2, V2> collector = new BSPMapperOutputCollector<K1, V1, K2, V2>(
        peer, memoryQueue, globalKeyDistribution[myId]);

    KeyValuePair<K1, V1> record = null;
    while ((record = peer.readNext()) != null) {
      map(record.getKey(), record.getValue(), collector);
    }

    Comparator<V2> valComparator = null;
    Configuration conf = peer.getConfiguration();

    Class<?> comparatorClass = conf.getClass(VALUE_COMPARATOR_CLASS, null);

    if (comparatorClass != null) {
      valComparator = (Comparator<V2>) ReflectionUtils.newInstance(
          comparatorClass, conf);
    }

    Reducer<K2, V2, K2, V2> combiner = null;
    Class<?> combinerClass = conf.getClass(COMBINER_CLASS, null);

    if (combinerClass != null) {
      combiner = (Reducer<K2, V2, K2, V2>) ReflectionUtils.newInstance(
          combinerClass, conf);
    }

    ExecutorService service = Executors.newFixedThreadPool(1);
    Future<Integer> future = service.submit(new CombineAndSortThread<K2, V2>(
        peer.getConfiguration(), this.memoryQueue, valComparator, combiner));

    String[] peers = peer.getAllPeerNames();

    IntWritable keyPartition = new IntWritable();
    LongWritable value = new LongWritable();

    WritableKeyValues<IntWritable, IntWritable> myIdTuple = new WritableKeyValues<IntWritable, IntWritable>(
        new IntWritable(peer.getPeerId()), new IntWritable(-1));

    int peerId = peer.getPeerId();
    for (int keyNumber = 0; keyNumber < globalKeyDistribution[0].length; ++keyNumber) {
      keyPartition.set(keyNumber);
      value.set(globalKeyDistribution[peerId][keyNumber]);
      myIdTuple.setValue(keyPartition);
      for (String peerName : peers) {
        peer.send(
            peerName,
            new WritableKeyValues<WritableKeyValues<IntWritable, IntWritable>, LongWritable>(
                myIdTuple, value));
      }
    }
    peer.save(KEY_DIST, this.globalKeyDistribution);
    peer.save(COMBINER_FUTURE, future);
    peer.save(MESSAGE_QUEUE, this.memoryQueue);
  }

  protected abstract void map(K1 key, V1 value,
      OutputCollector<K2, V2> collector) throws IOException;

}
