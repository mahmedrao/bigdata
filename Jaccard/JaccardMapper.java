import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DecimalFormat;
import java.util.Hashtable;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.TwoDArrayWritable;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
//import org.apache.commons.httpclient.URI;
//import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;



public class JaccardMapper extends Mapper<LongWritable, Text, IntWritable, Text2dArrayWritablePair> {
	
	Hashtable movieInfo = new Hashtable<String, String>();
	String[] genres, actors, entities;
	String[] attributes = new String[] {"genre", "actors", "directors", "country", "year", "ratings"};
	double p,q,r,s;
	double result = 0.0;
	String input[] = null;
	Set<String> keys;
	Text2dArrayWritablePair pair;
	int columnlength = 0;
	TwoDArrayWritables array2d = new TwoDArrayWritables();
	
	IntWritable[][] jaccard = new IntWritable[2][];
	

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
		
		p = 0;
		q = 0;
		r = 0;
		s = 0;
		
		/*
		 * dataset format
		 * 0 -> movieid
		 * 1 -> title
		 * 2 -> year
		 * 3 -> actors
		 * 4 -> directors
		 * 5 -> genre
		 * 6 -> country
		 * 7 -> ratings
		 * 8 -> cost
		 * 9 -> revenue
		 * */
		
		/*
		 * input format
		 * 0 -> genre
		 * 1 -> actors
		 * 2 -> directors
		 * 3 -> country
		 * 4 -> year
		 * 5 -> ratings
		 * */
		
		/*
		 * (q + r) / (p + q + r) 
		 * p -> number of variables positive for both objects 
		 * q -> number of variables positive for the ith objects and negative for jth objects
		 * r -> number of variables negative for the ith objects and positive for jth objects
		 * s -> number of variables negative for both objects
		 * */
		
		
		input = value.toString().toLowerCase().split(",");
		keys = movieInfo.keySet();
		
		
		//the jaccards 2d array column length depends on the user input best case is 6 but the worst case depends on the sub attributes count like more than one actor/director/genre/country.  
		columnlength = input[1].split("\\|").length + input[2].split("\\|").length + input[3].split("\\|").length + input[4].split("\\|").length + 2;
		jaccard = new IntWritable[2][columnlength];
		
		
		//initialize the 2d array
		for (int i = 0; i < jaccard.length; i++)
		{
			for (int j = 0; j < jaccard[i].length; j++)
			{
				jaccard[i][j] = new IntWritable(0);
			}
		}
		
		if (input.length > 0)
		{
			//iterate through the dataset in cache
			for(String keyy : keys)
			{
				//iterate to user's input attributes
				for (int attribute = 1; attribute < attributes.length; attribute++)
				{
					//if attribute available or not
					if (!input[attribute].equals("-")) 
					{
						entities = input[attribute].toLowerCase().split("\\|");
						int subattributecount = 0;
						
						for(String entity : entities) // iterate through entities i.e. genre, year, rating
						{
								//if entity matches
								if (movieInfo.get(keyy).toString().toLowerCase().contains(entity))
								{
									//if user criteria match with the data set, mark 1, 1
									jaccard[0][attribute + subattributecount] = new IntWritable(1);
									jaccard[1][attribute + subattributecount] = new IntWritable(1);
								}
								else
								{
									//if user criteria doesn't match with the data set, mark 1, 0
									jaccard[0][attribute + subattributecount] = new IntWritable(1);
									jaccard[1][attribute + subattributecount] = new IntWritable(0);
								}
								subattributecount += 1;
						}
					}
				}
				array2d.set(jaccard);
				pair = new Text2dArrayWritablePair(movieInfo.get(keyy).toString(), array2d);
			
				//emit [inputid, {movieinfo,2darray}]
				context.write(new IntWritable(Integer.parseInt(input[0].toString())), pair);
			}
			
			
		}
		}
	
	
	@Override
	  protected void setup(Context context) throws IOException, InterruptedException {
	    
		
		
	    // We know there is only one cache file, so we only retrieve that URI
	    java.net.URI fileUri = context.getCacheFiles()[0];
	    FileSystem fs = FileSystem.get(context.getConfiguration());
	    FSDataInputStream in = fs.open( new Path(fileUri) );
	    BufferedReader br = new BufferedReader(new InputStreamReader(in));
	    String line = null;
	    try {
	     // we discard the header row
	      br.readLine();
	      while ((line = br.readLine()) != null) {
	        String[] fields = line.split(",");
	        //id,whole_line
	          movieInfo.put(fields[0], line);
	      }
	      br.close();
	     } catch (IOException e1) {
	     }
	   super.setup(context);
	   }
}
