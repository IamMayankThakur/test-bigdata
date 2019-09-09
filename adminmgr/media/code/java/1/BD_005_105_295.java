//package example;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class BD_005_105_295{

	public static class iplMapper extends Mapper<LongWritable, Text , Text, Text>{

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

         	String valStr = value.toString();
		String[] valueStr= valStr.split(",");
     		if(valueStr[0].equals("ball"))
		{
                    	String keyUserID = valueStr[4]+","+valueStr[6];
			if(((valueStr[10].charAt(0)>= 65 && valueStr[10].charAt(0) <=90) ||  (valueStr[10].charAt(0)>= 97 && valueStr[10].charAt(0)<= 122 )) && (!(valueStr[9].equals("run out")) && !(valueStr[9].equals("retired hurt")))) 		  
			{
				context.write(new Text(keyUserID), new Text("1"));
			}
                    	else
			{
				context.write(new Text(keyUserID), new Text("0"));
			}

                }
            }
        }
    

    public static class iplCombiner extends Reducer<Text,Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            int ballCount = 0;
            int wicketCount = 0;
           
	    
            for (Text value : values) 
		{
                	
                	wicketCount += Integer.parseInt(value.toString()); 
                    	ballCount++;
                
                	
            	}
	
		if(ballCount>5)
		{	
			String op= Integer.toString(wicketCount) + "," + Integer.toString(ballCount);
			String result= key.toString()+","+op.trim();
			context.write(new Text("key"),new Text(result));
		}
        }
  }

	public static class iplReducer extends Reducer<Text,Text, Text, Text> {
		
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

	    List<ArrayList<String>> listoflists = new ArrayList<ArrayList<String>>();
		
            for (Text value : values) 
		{	
						
			String result = value.toString();
			String[] res_Str= result.split(",");	
			String bowl= res_Str[0];
			String bats= res_Str[1];
			String key_list= bowl + "," + bats;
			String runs=res_Str[2];
			String balls=res_Str[3];
                	
			
			ArrayList<String> list1 = new ArrayList<String>();
			list1.add(key_list);
			list1.add(runs);
			list1.add(balls);	
                	listoflists.add(list1);
                	
            	} 
	

	
	
	for (int i=0;i<listoflists.size()-1;i++)
	{
		for(int j=0;j<listoflists.size()-1-i;j++)
		{
			if(Integer.parseInt(listoflists.get(j).get(1)) < Integer.parseInt(listoflists.get(j+1).get(1)))
			{
				ArrayList<String> list1 = new ArrayList<String>();
				list1.add(listoflists.get(j).get(0));
				list1.add(listoflists.get(j).get(1));
				list1.add(listoflists.get(j).get(2));
				listoflists.set(j,listoflists.get(j+1));
				listoflists.set(j+1,list1);
		
			}
			else if(Integer.parseInt(listoflists.get(j).get(1)) == Integer.parseInt(listoflists.get(j+1).get(1)))
			{
					
				if(Integer.parseInt(listoflists.get(j).get(2)) > Integer.parseInt(listoflists.get(j+1).get(2)))
				{
					ArrayList<String> list1 = new ArrayList<String>();
					list1.add(listoflists.get(j).get(0));
					list1.add(listoflists.get(j).get(1));
					list1.add(listoflists.get(j).get(2));
					listoflists.set(j,listoflists.get(j+1));
					listoflists.set(j+1,list1);
				}
			}
		}
	}	
	

	
	for (int i=0;i<listoflists.size();i++)
	{
		ArrayList<String> list1 = new ArrayList<String>();
		list1 = listoflists.get(i);
		String key_fin = list1.get(0);
		String run_fin = list1.get(1);
		String ball_fin = list1.get(2);

		String val_fin = run_fin + "," + ball_fin;
		context.write(new Text(key_fin),new Text(val_fin));	
	}  
		
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

    }
}
