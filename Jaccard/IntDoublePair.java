import java.io.*;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import org.apache.hadoop.io.*;

public class IntDoublePair implements WritableComparable<IntDoublePair> {
	
	private IntWritable first;
	private DoubleWritable second;

	public IntDoublePair() {
		set(new IntWritable(), new DoubleWritable());
	}

	public IntDoublePair(int first, double second) {
		set(new IntWritable(first), new DoubleWritable(second));
	}

	public void set(IntWritable first, DoubleWritable second) {
		this.first = first;
		this.second = second;
	}

	public IntWritable getFirst() {
		return first;
	}

	public DoubleWritable getSecond() {
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
		if (o instanceof IntDoublePair) {
			IntDoublePair tp = (IntDoublePair) o;
			return first.equals(tp.first) && second.equals(tp.second);
		}
		return false;
	}

	@Override
	public String toString() {
		return first + "\t" + second;
	}

	@Override
	public int compareTo(IntDoublePair tp) {
		
		
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