import java.io.*;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import org.apache.hadoop.io.*;

public class Text2dArrayWritablePair implements WritableComparable<Text2dArrayWritablePair> {
	
	private Text first;
	private TwoDArrayWritables second;

	public Text2dArrayWritablePair() {
		set(new Text(), new TwoDArrayWritables());
	}

	public Text2dArrayWritablePair(String first, TwoDArrayWritables second) {
		set(new Text(first), second);
	}

	public void set(Text first, TwoDArrayWritables second) {
		this.first = first;
		this.second = second;
	}

	public Text getFirst() {
		return first;
	}

	public TwoDArrayWritables getSecond() {
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
		if (o instanceof Text2dArrayWritablePair) {
			Text2dArrayWritablePair tp = (Text2dArrayWritablePair) o;
			return first.equals(tp.first) && second.equals(tp.second);
		}
		return false;
	}

	@Override
	public String toString() {
		return first + "\t" + second;
	}

	@Override
	public int compareTo(Text2dArrayWritablePair o) {
		// TODO Auto-generated method stub
		return 0;
	}

	/*@Override
	public int compareTo(Text2dArrayWritablePair tp) {
		
		
		int cmp = first.compareTo(tp.first);
		if (cmp != 0) {
			return cmp;
		}
		return ((Comparable<Text2dArrayWritablePair>) second).compareTo(tp.second);
	}*/
	
	
	}