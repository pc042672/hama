package org.apache.hama.computemodel.mapreduce;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.io.serializer.WritableSerialization;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.Task.CombineOutputCollector;
import org.apache.hadoop.mapred.Task.CombinerRunner;
import org.apache.hadoop.mapred.Task.TaskReporter;
import org.apache.hadoop.mapred.lib.HashPartitioner;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.bsp.OutputCollector;
import org.apache.hama.bsp.Superstep;
import org.apache.hama.util.KeyValuePair;


public abstract class Mapper<K1, V1, K2, V2, M> 
  extends Superstep<K1, V1, K2, V2, Writable> {

  public static final String KEY_COMPARATOR_CLASS = "hama.mapreduce.keycompare";
  public static final String MAPPER_CLASS = "hama.mapreduce.mapclass";
  public static final String REDUCER_CLASS = "hama.mapreduce.reduceclass";
  
  Map<Integer, Long> keyDistributionMap = new HashMap<Integer, Long>();
  
  public static class MapperOutputCollector<K1, V1, K2, V2> implements OutputCollector<K2,V2>{

    BSPPeer<K1, V1, K2, V2, Writable> bspPeer;
    Partitioner<K2, V2> partitioner; //= //ReflectionUtils.newInstance(, arg1)
                            //new HashPartitioner<K2, V2>();
    final int partitions;
    final JobConf job;
    final Class<K2> keyClass;
    final Class<V2> valClass;
    final RawComparator<K2> comparator;
    final SerializationFactory serializationFactory;
    final Serializer<K2> keySerializer;
    final Serializer<V2> valSerializer;
    final CombineOutputCollector<K2, V2> combineCollector;
    final Configuration jobConf;
    
    public MapperOutputCollector(BSPPeer<K1, V1, K2, V2, Writable> peer) {
      bspPeer = peer;
      jobConf = peer.getConfiguration();
      comparator = (RawComparator<K2>) ReflectionUtils.newInstance(
          jobConf.getClass(KEY_COMPARATOR_CLASS, RawComparator.class), jobConf); 
      keyClass = (Class<K2>) jobConf.getClass(MAPPER_CLASS, null);
      valClass = (Class<V2>) jobConf.getClass(REDUCER_CLASS, null);
      serializationFactory = new SerializationFactory(jobConf);
      keySerializer = serializationFactory.getSerializer(keyClass);
    }

    @Override
    public void collect(K2 key, V2 value) throws IOException {
      
      KeyValuePair<K2,V2> keyVal = new KeyValuePair<K2, V2>(key, value);
      
      
      
      bspPeer.send(bspPeer.getPeerName(partitioner.getPartition(
          key, value, bspPeer.getNumPeers())), new KeyValuePair<K2, V2>(key, value));
      
      
      
      
      
    }

  }

  @Override
  protected void compute(BSPPeer<K1, V1, K2, V2, Writable> peer)
      throws IOException {
    OutputCollector<K1, V1> collector = new MapperOutputCollector<K1, V1, K2, V2>(peer);
    KeyValuePair<K1, V1> record = null;
    while((record = peer.readNext()) != null){
      map(record.getKey(),record.getValue(),collector);
    }


  }

  protected abstract void map(K1 key,V1 value,OutputCollector<K1,V1> collector);

}
