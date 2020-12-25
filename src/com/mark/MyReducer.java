package com.mark;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MyReducer extends Reducer<IntWritable, Text, IntWritable, IntWritable> {
	
	protected void reduce(IntWritable row, Iterable<Text> numit, Context c) throws IOException, InterruptedException

	{
		
		
		//int max_value=Integer.MIN_VALUE; //-2^31
		
		int maxValue=0;
		
		for(Text each_index:numit) //first time line is "1,2,3,4"
		{
			String[] nums=each_index.toString().split(" "); // ["1","2","3","4"]
			
			for(String each_num:nums)
			{
				int numVal=Integer.parseInt(each_num); //converting string to integer
				 
				if(numVal>maxValue)
				{
					maxValue=numVal; //Initializing maxValue with recent big(numVal) value
				}
			}
			
		}
		
		c.write(row, new IntWritable(maxValue));

	}

}
