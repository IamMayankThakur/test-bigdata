//package example;
import java.util.*;
import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class BD_005_105_295{

	static String venueID1="";
	static String venue="";
	static String batsman="";
	static String strike="";		
	static String runs="";

    	public static class iplMapper extends Mapper<LongWritable, Text , Text, Text>{

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

         	String valStr = value.toString();
		String[] valueStr= valStr.split(",");
		String regex="\\s+$";

		if(valueStr[0].charAt(0) =='i')
		{
			
			if(valueStr[1].equals("venue"))
			{
			 	String stadium = valueStr[2].toString();

			 	if(stadium.charAt(0)=='"')		
				{
					stadium = stadium + "," + valueStr[3].toString();
					StringBuilder sb = new StringBuilder(stadium);
					stadium = sb.toString();
					String venueID=stadium;
				 	venueID1=venueID.trim();	
				}

				else
				{
		            	 	String venueID=valueStr[2];
					 venueID1=venueID.trim();
				}				
                   	}                    	
			
                    	

                }     		

		else if(valueStr[0].charAt(0) =='b')
		{
			if(Integer.parseInt(valueStr[8])==0)
			{
                    		String keyUserID = venueID1+";"+valueStr[4];
				String keyUserID1=keyUserID.trim();
				String keyUserID2=keyUserID1;
			
				String result = valueStr[7].trim();
				context.write(new Text(keyUserID2), new Text(result));
			}
                    	

                }
            }
        }
    

	public static class iplCombiner extends Reducer<Text,Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            int ballCount = 0;
            int runCount = 0;
           
	    
            for (Text value : values) 
		{
                	
                	
                    	runCount += Integer.parseInt(value.toString()); 
                    	ballCount++;
                }
		
		if(ballCount>=10)
		{	
			float sr = (float)(runCount*100)/ballCount;
			String op= Float.toString(sr)+";"+ Integer.toString(runCount);
			String result= key.toString()+";"+op.trim();
			
            		context.write(new Text("key"),new Text(result));
		}
            }
  	}

	public static class iplReducer extends Reducer<Text,Text, Text, Text> {	
		
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		
            for (Text value : values) 
		{	
						
			String result = value.toString();
			String[] res_Str= result.split(";");
			if(venue.equals(""))
			{
				venue= res_Str[0];
				batsman= res_Str[1];
				strike= res_Str[2];
				runs= res_Str[3];				
			
			}
			else if(!(venue.equals(res_Str[0])))
			{
				context.write(new Text(venue),new Text(batsman));
				venue= res_Str[0];
				batsman= res_Str[1];
				strike= res_Str[2];
				runs= res_Str[3];
			}
			else
			{
				if(Float.parseFloat(res_Str[2])>Float.parseFloat(strike))
				{
					batsman= res_Str[1];
					strike= res_Str[2];
					runs= res_Str[3];
				}
				else if(Float.parseFloat(res_Str[2])==Float.parseFloat(strike))
				{
					if(Integer.parseInt(res_Str[3]) > Integer.parseInt(runs))
					{
						batsman= res_Str[1];
						strike= res_Str[2];
						runs= res_Str[3];
					}
				}
			}	
			
                	
			
                	
            	} 

		context.write(new Text(venue),new Text(batsman));
	}
  }


	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		conf.set("mapred.textoutputformat.separator",",");
		Job job = Job.getInstance(conf, "ipl");

		job.setJarByClass(BD_005_105_295.class);
		job.setMapperClass(iplMapper.class);

		job.setCombinerClass(iplCombiner.class);
		job.setReducerClass(iplReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0:1);

    		}//end of main

	}//end of q4
