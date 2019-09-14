import java.util.Map;
import java.util.TreeMap;
import java.util.*;
import java.util.Scanner;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapred.*;

import java.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
public class BD_1632_2146_2187 { 

    public static void main(String[] args) throws Exception 
    { 
       
        Configuration conf = new Configuration(); 
        conf.set("mapred.textoutputformat.separator",",");
        String[] otherArgs = new GenericOptionsParser(conf, 
                                args).getRemainingArgs(); 

        if (otherArgs.length < 2) 
        { 
            System.err.println("Error: please provide two paths"); 
            System.exit(2); 
        } 

        Job job = Job.getInstance(conf, "please..."); 
        job.setJarByClass(BD_1632_2146_2187.class); 

        job.setMapperClass(mapper_task4.class); 
        job.setReducerClass(reducer_task4.class); 

        job.setMapOutputKeyClass(Text.class); 
        job.setMapOutputValueClass(IntWritable.class); 

        job.setOutputKeyClass(Text.class); 
        job.setOutputValueClass(Text.class); 

        FileInputFormat.addInputPath(job, new Path(otherArgs[0])); 
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1])); 

        System.exit(job.waitForCompletion(true) ? 0 : 1); 
    } 
} 



class mapper_task4 extends Mapper<Object,Text,Text, IntWritable> { 


	String venue;
	String batsman;
	

	public void setup(Context context) throws IOException, 
									InterruptedException 
	{ 
		int flag =  0;
	} 


	
	public void map(Object key, Text value, 
	Context context) throws IOException, 
					InterruptedException 
	{ 
		int cur_run;
		String[] tokens = value.toString().split(","); 
		
		if(tokens[0].equals("info")&&tokens[1].equals("venue")){

			if(tokens[2].startsWith("\""))
				venue=tokens[2] + "," + tokens[3];
			else
				venue = tokens[2];
	
		}
		else if(tokens[0].equals("ball") && tokens[8].equals("0") && tokens.length >=8)
		{
			cur_run =Integer.parseInt(tokens[7]);
			batsman = tokens[4];
			context.write(new Text(venue+":"+batsman) ,new IntWritable(cur_run));
		}
	
	} 
}

class reducer_task4 extends Reducer<Text,IntWritable,Text,Text> { 

	private Map<String, String > hash_map;  
	private Map<String,String> tree_map;
	
	
	public void setup(Context context) throws IOException, 
									InterruptedException 
	{ 
		hash_map = new HashMap<>();
		tree_map = new TreeMap<>();
	} 

	
	public void reduce(Text key, Iterable<IntWritable> values, 
	Context context) throws IOException, InterruptedException 
	{
		int ball = 0;
		int sum = 0;
		float sr;
		for(IntWritable val:values){
			sum += val.get();	//runs
			ball +=1;
		}
		if(ball >=10){
			sr = (sum * 100) /ball;
			String[] temp = key.toString().split(":");
			
			String test= temp[1] + "," +String.valueOf(sr)+","+String.valueOf(sum);

			if(hash_map.containsKey(temp[0]))
			{
				//String[] b = hash_map.get(temp[0]).split(",");
				String[] b = hash_map.get(temp[0]).split(",");
				if(sr > Float.valueOf(b[1]))
				{
					
					hash_map.put(temp[0],test);
				}
				else if(sr == Float.valueOf(b[1]))
				{
					//if(Integer.valueOf(b[2]) >= sum )
					if( sum > Integer.valueOf(b[2]))
					{
						hash_map.put(temp[0],test);
					}
				}
			}
			else{
				hash_map.put(temp[0],test);
			}
	 
		} 
	}

	
	public void cleanup(Context context) throws IOException, 
									InterruptedException 
	{
		//System.out.println(hash_map);
		tree_map.putAll(hash_map);
		for(Map.Entry<String, String> entry : tree_map.entrySet())
		{
			String[] x = entry.getValue().split(",");

			context.write(new Text(entry.getKey()) , new Text(x[0] ) );
		}

		
			
	}	
} 



