import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;





public class JaccardReducer extends Reducer<IntWritable, Text2dArrayWritablePair, Text, Text> {
	
	double p,q,r,s;
	double result = 0.0;
	Writable[][] jaccard;
	int temp1 = 0, temp2 = 0;
	ArrayList<IntDoubleTextPair> resultCollection = new ArrayList<IntDoubleTextPair>();
	IntDoubleTextPair temp = new IntDoubleTextPair();
	DecimalFormat df;
	IntDoubleTextPair pair;
	
	//Jaccard distance 1.0 emplies that the user's criteria doesn't meet at all
	public static final double nonacceptvalue = 1.0;
	
	
	public void reduce(IntWritable key, Iterable<Text2dArrayWritablePair> values, Context context) throws IOException, InterruptedException
	{
		resultCollection = new ArrayList<IntDoubleTextPair>();
	
		//assign values to Jaccard's variables 
		for (Text2dArrayWritablePair value : values) 
		{
			//retrieve 2darray
			jaccard = value.getSecond().get();
			
			for (int column = 0; column < jaccard[0].length; column++)
			{
				temp1 = Integer.parseInt(jaccard[0][column].toString());
				temp2 = Integer.parseInt(jaccard[1][column].toString());
				
					if (temp1 == 1 & temp2 == 1)
					{
						p += 1;
					}
					else if (temp1 == 1 & temp2 == 0)
					{
						q += 1;
					}
					else if (temp1 == 0 & temp2 == 1)
					{
						r += 1;
					}
					else if (temp1 == 0 & temp2 == 0)
					{
						s += 1;
					}
			}
			
			//apply formula
			result = (double) ((q + r) / (p + q + r));
			
			//reset the values
			p = 0;
			q = 0;
			r = 0;
			s = 0;
			
			
			if (result < nonacceptvalue)// if Jaccard's distance is less than the nonacceptvalue i.e. 1 implies that user input doesn't meet the specific movie detail
			{
				//emit ([userinputid, ranking] , title of movie)
				df = new DecimalFormat("#.##");

				pair = new IntDoubleTextPair(key.get(), Double.parseDouble(df.format(result)), value.getFirst().toString().split(",")[1]);
				//assign the result to collection
				resultCollection.add(pair);
			}
		
		}
		
		
		//sort the collection
		resultCollection = sortCollection(resultCollection);
		for (IntDoubleTextPair result : resultCollection) 
		{
			context.write(new Text(result.toString()), result.getThird());	
		}
		

	
		
		
	}

	private ArrayList<IntDoubleTextPair> sortCollection(ArrayList<IntDoubleTextPair> collection)
	{
		for (int current = 0; current < collection.size(); current++)
		{
			for (int next = 1; next < collection.size(); next++)
			{
				//if userid is greater or equal
				if (collection.get(current).getFirst().get() >= collection.get(next).getFirst().get())
				{
					//if userid is greater or equal
				      if (collection.get(current).getFirst().get() == collection.get(next).getFirst().get())
				      {
							//if Jaccard's distance is greater or equal
				          if (collection.get(current).getSecond().get() > collection.get(next).getSecond().get())
				          {
				        	  temp = new IntDoubleTextPair();
				        	  
				        	  //swap
				        	  temp.set(collection.get(next).getFirst(), collection.get(next).getSecond(), collection.get(next).getThird());
				        	  
		                      collection.set(next, collection.get(current));
					          collection.set(current, temp);
				          }
				      }

				      else
				      {
				    	  
				    	  temp = new IntDoubleTextPair();
				    	  
			        	  //swap
				    	  temp.set(collection.get(next).getFirst(), collection.get(next).getSecond(), collection.get(next).getThird());
				          collection.set(next, collection.get(current));
				          collection.set(current, temp);
				      }
				  }			
			}
		}
		return collection;
	}
}
