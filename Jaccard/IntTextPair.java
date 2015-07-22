import java.io.*;

import org.apache.hadoop.io.*;

public class IntTextPair implements WritableComparable<IntTextPair> {

	private IntWritable first;
	private Text second;

	public IntTextPair() {
		set(new IntWritable(), new Text());
	}

	public IntTextPair(int first, String second) {
		set(new IntWritable(first), new Text(second));
	}

	public void set(IntWritable first, Text second) {
		this.first = first;
		this.second = second;
	}

	public IntWritable getFirst() {
		return first;
	}

	public Text getSecond() {
		return second;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		first.write(out);
		second.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		first.readFields(in);
		second.readFields(in);
	}

	@Override
	public int hashCode() {
		return first.hashCode() * 163 + second.hashCode();
	}

	@Override
	public boolean equals(Object o) {
		if (o instanceof IntTextPair) {
			IntTextPair tp = (IntTextPair) o;
			return first.equals(tp.first) && second.equals(tp.second);
		}
		return false;
	}

	@Override
	public String toString() {
		return "[" + first + "\t" + second + "]";
	}

	@Override
	public int compareTo(IntTextPair tp) {
		int cmp = first.compareTo(tp.first);
		if (cmp != 0) {
			return cmp;
		}
		return second.compareTo(tp.second);
	}
}