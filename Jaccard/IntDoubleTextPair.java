import java.io.*;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import org.apache.hadoop.io.*;

public class IntDoubleTextPair implements Comparable<IntDoubleTextPair>, WritableComparable<IntDoubleTextPair> {
	
	private IntWritable first;
	private DoubleWritable second;
	private Text third;

	public IntDoubleTextPair() {
		set(new IntWritable(), new DoubleWritable(), new Text());
	}

	public IntDoubleTextPair(int first, double second, String third) {
		set(new IntWritable(first), new DoubleWritable(second), new Text(third));
	}

	public void set(IntWritable first, DoubleWritable second, Text third) {
		this.first = first;
		this.second = second;
		this.third = third;
	}

	public IntWritable getFirst() {
		return first;
	}

	public DoubleWritable getSecond() {
		return second;
	}

	public Text getThird() {
		return third;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		first.write(out);
		second.write(out);
		third.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		first.readFields(in);
		second.readFields(in);
		third.readFields(in);
	}

	@Override
	public int hashCode() {
		return first.hashCode() * 163 + second.hashCode() * 165 + third.hashCode();
	}

	@Override
	public boolean equals(Object o) {
		if (o instanceof IntDoubleTextPair) {
			IntDoubleTextPair tp = (IntDoubleTextPair) o;
			return first.equals(tp.first) && second.equals(tp.second) && third.equals(tp.third);
		}
		return false;
	}

	@Override
	public String toString() {
		return first + "\t" + second;
	}

	@Override
	public int compareTo(IntDoubleTextPair tp) {
		
		
		int cmp = first.compareTo(tp.first);
		if (cmp != 0) {
			return cmp;
		}
		return second.compareTo(tp.second);
	}
	
	
/*	public static Comparator<IntDoubleTextPair> IntComparator = new Comparator<IntDoubleTextPair>() {
		
		@Override
		public int compare(IntDoubleTextPair o1, IntDoubleTextPair o2) {
			// TODO Auto-generated method stub
		      return o1.first.compareTo(o2.first);
		}
	};*/
	}