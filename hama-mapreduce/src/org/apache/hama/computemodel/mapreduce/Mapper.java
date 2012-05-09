package org.apache.hama.computemodel.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.lib.HashPartitioner;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.bsp.OutputCollector;
import org.apache.hama.bsp.Superstep;
import org.apache.hama.bsp.sync.SyncException;
import org.apache.hama.util.KeyValuePair;

public abstract class Mapper<K1, V1, K2 extends Writable, V2 extends Writable>
    extends
    Superstep<K1, V1, K2, V2, WritableKeyValues<? extends Writable, ? extends Writable>> {

  public static final String VALUE_COMPARATOR_CLASS = "hama.mapreduce.valuecompare";
  public static final String COMBINER_CLASS = "hama.mapreduce.combiner";
  public static final String MESSAGE_QUEUE = "MESSAGE_QUEUE";
  public static final String KEY_DIST = "KEY_DISTRIBUTION";
  public static final String COMBINER_FUTURE = "COMBINER_FUTURE";
  private Map<Integer, Long> keyDistributionMap = new HashMap<Integer, Long>();
  private long[][] globalKeyDistribution;
  private PriorityQueue<WritableKeyValues<K2, V2>> memoryQueue;

  private static class CombineAndSortThread<K2, V2> implements
      Callable<Integer> {

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

      while (recordIterator.hasNext()) {

        WritableKeyValues<K2, V2> record = recordIterator.next();
        K2 key = record.getKey();
        if (previousRecord != null && combinerInstance != null
            && key.equals(previousRecord.getKey())) {
          previousRecord.addValue(record.getValue());
          recordIterator.remove();
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

  public static class BSPMapperOutputCollector<K1, V1, K2, V2> implements
      OutputCollector<K2, V2> {

    BSPPeer<K1, V1, K2, V2, WritableKeyValues<? extends Writable, ? extends Writable>> bspPeer;
    final int partitions;
    final Configuration job;
    PriorityQueue<WritableKeyValues<K2, V2>> collectorQueue;
    Partitioner<K2, V2> partitioner;
    Map<Integer, Long> keyDistributionMap;
    long[] keyDistribution;

    // SortedMessageQueue<ByteWritable> sortedQueue;

    public BSPMapperOutputCollector(
        BSPPeer<K1, V1, K2, V2, WritableKeyValues<? extends Writable, ? extends Writable>> peer,
        PriorityQueue<WritableKeyValues<K2, V2>> diskQueue,
        long[] peerKeyDistribution) {
      bspPeer = peer;
      this.job = peer.getConfiguration();
      this.partitions = peer.getNumPeers();
      this.collectorQueue = diskQueue;
      this.partitioner = (Partitioner<K2, V2>) ReflectionUtils.newInstance(
          job.getClass("", HashPartitioner.class), job);
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

  private static class CombinerOutputCollector<K, V> implements
      org.apache.hadoop.mapred.OutputCollector<K, V> {

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
      BSPPeer<K1, V1, K2, V2, WritableKeyValues<? extends Writable, ? extends Writable>> peer)
      throws IOException {

    this.memoryQueue = new PriorityQueue<WritableKeyValues<K2, V2>>();
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

    Iterator<Integer> keyIter = keyDistributionMap.keySet().iterator();
    IntWritable keyPartition = new IntWritable();
    LongWritable value = new LongWritable();

    WritableKeyValues<IntWritable, IntWritable> myIdTuple = new WritableKeyValues<IntWritable, IntWritable>(
        new IntWritable(peer.getPeerId()), new IntWritable(-1));

    while (keyIter.hasNext()) {
      int keyNumber = keyIter.next();
      keyPartition.set(keyNumber);
      value.set(keyDistributionMap.get(keyNumber));
      myIdTuple.setValue(keyPartition);
      for (String peerName : peers) {
        peer.send(
            peerName,
            new WritableKeyValues<WritableKeyValues<IntWritable, IntWritable>, LongWritable>(
                myIdTuple, value));
      }
    }

    try {
      peer.sync();
    } catch (SyncException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    peer.save(MESSAGE_QUEUE, this.memoryQueue);
    peer.save(KEY_DIST, this.globalKeyDistribution);
    peer.save(COMBINER_FUTURE, future);
  }

  protected abstract void map(K1 key, V1 value,
      OutputCollector<K2, V2> collector);

}