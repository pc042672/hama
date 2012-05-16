package org.apache.hama.computemodel.mapreduce;

import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.BSPJob;
import org.apache.hama.bsp.BSPJobClient;
import org.apache.hama.bsp.ClusterStatus;
import org.apache.hama.bsp.NullInputFormat;
import org.apache.hama.bsp.OutputCollector;
import org.apache.hama.bsp.TextOutputFormat;

public class WordCount {

  public static class WordCountMapper extends
      Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value,
        OutputCollector<Text, IntWritable> collector) throws IOException {
      // TODO Auto-generated method stub
      String line = value.toString();
      StringTokenizer itr = new StringTokenizer(line);
      while (itr.hasMoreTokens()) {

        // Version 0.1 has this limitation.
        IntWritable one = new IntWritable(1);
        Text word = new Text();

        word.set(itr.nextToken());
        collector.collect(word, one);
      }
    }
  }

  public static class WordCountReducer extends
      Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    public void reduce(Text key, Iterator<IntWritable> values,
        org.apache.hadoop.mapred.OutputCollector<Text, IntWritable> output)
        throws IOException {
      int sum = 0;
      while (values.hasNext()) {
        sum += values.next().get();
      }
      output.collect(key, new IntWritable(sum));

    }
  }

  public static class SimplePartitioner extends Partitioner<Text, IntWritable> {

    @Override
    public int getPartition(Text key, IntWritable value, int numPartitions) {
      // TODO Auto-generated method stub
      return (Character.toUpperCase(key.charAt(0)) - 'A') % numPartitions;
    }

  }

  public static class IntComparator implements Comparator<IntWritable> {

    @Override
    public int compare(IntWritable o1, IntWritable o2) {
      // TODO Auto-generated method stub
      return o1.compareTo(o2);
    }

  }

  /**
   * @param args
   * @throws IOException
   * @throws InterruptedException
   * @throws ClassNotFoundException
   */
  public static void main(String[] args) throws IOException,
      ClassNotFoundException, InterruptedException {
    // TODO Auto-generated method stub

    HamaConfiguration conf = new HamaConfiguration();
    BSPJob bsp = new BSPJob(conf, WordCountMapper.class);

    bsp.setJobName("MapReduce - WordCount");

    MapRedConf mapRedConfig = new MapRedConf();
    mapRedConfig.setMapperClass(WordCountMapper.class);
    mapRedConfig.setCombinerClass(WordCountReducer.class);
    mapRedConfig.setReducerClass(WordCountReducer.class);
    mapRedConfig.configureBSPJob(bsp);

    bsp.setInputFormat(NullInputFormat.class);
    bsp.setOutputKeyClass(Text.class);
    bsp.setOutputValueClass(DoubleWritable.class);
    bsp.setOutputFormat(TextOutputFormat.class);
    
    BSPJobClient jobClient = new BSPJobClient(conf);
    ClusterStatus cluster = jobClient.getClusterStatus(true);

    if (args.length > 0) {
      bsp.setInputPath(new Path(args[0]));
      bsp.setOutputPath(new Path(args[1]));
    } else {
      // Set to maximum
      throw new IllegalArgumentException("Please enter input and output paths.");
    }
    long startTime = System.currentTimeMillis();
    if (bsp.waitForCompletion(true)) {
      System.out.println("Job Finished in "
          + (double) (System.currentTimeMillis() - startTime) / 1000.0
          + " seconds");
    }

  }

}
