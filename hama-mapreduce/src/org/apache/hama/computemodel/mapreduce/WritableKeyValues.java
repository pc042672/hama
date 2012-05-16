package org.apache.hama.computemodel.mapreduce;

import java.io.DataInput;

public class WritableKeyValues<K extends WritableComparable, V extends Writable>
    implements WritableComparable<WritableKeyValues<K, V>> {

  private K key;
  private List<V> valueList;

  public WritableKeyValues() {

  }

  public WritableKeyValues(K key, V value) {

    this.key = key;
    this.valueList = new ArrayList<V>();
    this.valueList.add(value);

  }

  public K getKey() {
    return this.key;
  }

  public V getValue() {
    return this.valueList.get(0);
  }

  public List<V> getValues() {
    return this.valueList;
  }

  public void setKey(K key) {
    this.key = key;
  }

  public void setValue(V value) {
    this.valueList.clear();
    this.valueList.add(value);
  }

  public void addValue(V value) {
    if (this.valueList == null) {
      valueList = new ArrayList<V>();
    }
    valueList.add(value);
  }

  public void addValues(List<V> values) {
    this.valueList.addAll(values);
  }

  @Override
  public void write(DataOutput out) throws IOException {

    out.writeUTF(this.key.getClass().getCanonicalName());
    this.key.write(out);
    out.writeLong(this.valueList.size());
    if (valueList.size() != 0) {
      out.writeUTF(this.valueList.get(0).getClass().getCanonicalName());

      Iterator<V> iter = this.valueList.iterator();
      while (iter.hasNext()) {
        iter.next().write(out);
      }
    }

  }

  @Override
  public void readFields(DataInput in) throws IOException {
    String keyClass = in.readUTF();
    try {
      this.key = ReflectionUtils.newInstance(keyClass, null);
    } catch (ClassNotFoundException ce) {
      throw new IOException(ce);
    }
    this.key.readFields(in);
    int size = in.readInt();
    if (size > 0) {
      String valueClass = in.readUTF();
      try {
        this.valueList = new ArrayList<V>(size);
        for (int i = 0; i < size; ++i) {
          V value = ReflectionUtils.newInstance(valueClass, null);
          value.readFields(in);
          this.valueList.add(value);
        }
      } catch (ClassNotFoundException ce) {
        throw new IOException(ce);
      }

    }

  }

  @Override
  public int compareTo(WritableKeyValues<K, V> o) {
    // TODO Auto-generated method stub
    return this.key.compareTo(o.getKey());
  }

  public void combine(Reducer<K, V, K, V> combiner,
      Comparator<V> valueComparator, OutputCollector<K, V> collector)
      throws IOException {

    if (valueList == null)
      return;

    if (valueComparator != null) {
      Collections.sort(this.valueList, valueComparator);
    }

    combiner.reduce(key, this.valueList.iterator(), collector, null);
  }

  public void sortValues(Comparator<V> valueComparator) {
    Collections.sort(this.valueList, valueComparator);
  }

}
