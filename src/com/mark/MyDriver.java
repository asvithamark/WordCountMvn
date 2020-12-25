package com.mark;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class MyDriver {
	

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		
		String input_path="hdfs://localhost:9000/usr/markandeyuluinturi/RowWiseHighest";
		String output_path="hdfs://localhost:9000/usr/markandeyuluinturi/RowWiseHighest_output";
		
		
		Path ipath=new Path(input_path); //pointing to input path
		Path opath=new Path(output_path); //pointing to output path
		
		Configuration conf=new Configuration(); //map reduce program should pointing to some configuration file
        Job job=Job.getInstance(conf); //creating job object
        
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class); //Attaching Mapper and Reducer classes to Job
        job.setJarByClass(MyDriver.class);
        
        job.setOutputKeyClass(IntWritable.class); //key from mapper
        job.setOutputValueClass(Text.class); //value from mapper
        
        
      //Configuring input location and output location
        
 		FileInputFormat.addInputPath(job, ipath); //linking path and job that is my job should run on this input path
 		FileOutputFormat.setOutputPath(job,opath);

 		opath.getFileSystem(conf).delete(opath, true);
 		System.exit(job.waitForCompletion(true)? 0:1);// Success 0 otherwise abnormal termination 1
 		

	}

}
