import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.TwoDArrayWritable;


public class TwoDArrayWritables extends TwoDArrayWritable
{
	//define the default constructor for TwoDArrayWritable in order to emit TwoDArrayWritable 
	public TwoDArrayWritables() {
        super(IntWritable.class);
        
	}
	
	
	public TwoDArrayWritables(Class valueClass) {
		super(valueClass);
		// TODO Auto-generated constructor stub
	}
	
	
}
