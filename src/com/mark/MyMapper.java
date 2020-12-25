package com.mark;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class MyMapper extends Mapper<LongWritable,Text,IntWritable,Text>{
	
	int row=0;
	@Override
	protected void map(LongWritable k, Text v, Context c)
			throws IOException, InterruptedException 
	{ 
		row++;
		c.write(new IntWritable(row), new Text(v));
		

	}

}
