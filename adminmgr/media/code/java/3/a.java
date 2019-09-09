import org.apache.hadoop.conf.Configuration; 
import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.io.IntWritable; 
import org.apache.hadoop.io.Text; 
import org.apache.hadoop.mapreduce.Job; 
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat; 
import org.apache.hadoop.util.GenericOptionsParser; 
import java.io.*; 
import java.util.*; 
import java.util.Map; 
import org.apache.hadoop.io.Text; 
import org.apache.hadoop.io.IntWritable; 
import org.apache.hadoop.mapreduce.Mapper; 

import java.io.IOException; 
import java.util.Map; 
import java.util.TreeMap; 
import java.util.*;
import java.util.Scanner;

import org.apache.hadoop.io.IntWritable; 
import org.apache.hadoop.io.Text; 
import org.apache.hadoop.mapreduce.Reducer; 
import org.apache.hadoop.mapred.*;

public class a
{
public class mapper_task4 extends Mapper<Object, 
							Text, Text, IntWritable> { 


	String venue;
	String batsman;
	
	@Override
	public void setup(Context context) throws IOException, 
									InterruptedException 
	{ 
		int flag =  0;
	} 


	@Override
	public void map(Object key, Text value, 
	Context context) throws IOException, 
					InterruptedException 
	{ 
		int cur_run;
		String[] tokens = value.toString().split(","); 

		if(tokens[1].equals("venue")){

			venue =tokens[2];
		}
		else if(tokens[0].equals("ball") && tokens[8].equals("0") && tokens.length >=8)
		{
			cur_run =Integer.parseInt(tokens[7]);
			batsman = tokens[4];
			context.write(new Text(venue+","+batsman) ,new IntWritable(cur_run));
		}
	
	} 

	@Override
	public void cleanup(Context context) throws IOException, 
									InterruptedException 
	{ 
		// for(Map.Entry<String, int[]> entry : hash_map.entrySet())
		// 	{

		// 		context.write(new Text(venue),new Text(entry.getKey() + ","+ entry.getValue()[0] +","+entry.getValue()[1]) );
		// 	}
		// 	hash_map.clear();
			
	} 
} 	


public class reducer_task4 extends Reducer<Text, 
					IntWritable,Text,Text> { 

	private Map<String, String > hash_map;  
	private Map<String,String> tree_map;
	
	@Override
	public void setup(Context context) throws IOException, 
									InterruptedException 
	{ 
		hash_map = new HashMap<>();
		tree_map = new TreeMap<>();
	} 

	//@Override
	public void reduce(Text key, Iterable<IntWritable> values, 
	Context context) throws IOException, InterruptedException 
	{
		int ball = 0;
		int sum = 0;
		float sr;
		for(IntWritable val:values){
			sum += val.get();
			ball +=1;
		}
		if(ball >=10){
			sr = (sum * 100) /ball;
			String[] temp = key.toString().split(",");
			
			String test= temp[1] + "," +String.valueOf(sr)+","+String.valueOf(sum);

			if(hash_map.containsKey(temp[0]))
			{
				String[] b = hash_map.get(temp[0]).split(",");
				if(sr > Float.valueOf(b[1]))
				{
					
					hash_map.put(temp[0],test);
				}
				else if(sr == Float.valueOf(b[1]))
				{
					if(Integer.valueOf(b[2]) <= sum )
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

	@Override
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
        job.setJarByClass(a.class); 

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
