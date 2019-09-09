//package com.bigdata.ipl_4;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class BD_186_281_1583 {	
    // Map function
    public static class MyMapper extends Mapper<LongWritable, Text, Text, Text>{
    	// word -> final key ; output from map
    	 private Text pair = new Text();
    	 private Text Runs = new Text();
    	 public void map(LongWritable key, Text value, Context context) 
                 throws IOException, InterruptedException {
             // Splitting the line on comma
             String[] lineArr = value.toString().split(",");
             
             if(lineArr[0].equals("info") && lineArr[1].equals("venue")) {
            	 String stadium = lineArr[2].toString();
            	 if(stadium.charAt(0) == '"') {
            		 stadium = stadium + "," + lineArr[3].toString();
            		// StringBuilder sb = new StringBuilder(stadium);
            		 //sb.deleteCharAt(0);
            		 //sb.deleteCharAt(stadium.length()-1);
            		 //stadium = sb.toString();
            	 }
            	 CurrentVenue.setVenue(stadium);
             }
             
             else if(lineArr[0].equals("ball") && (Integer.parseInt(lineArr[8]) == 0)) {
            	 
            	 String v = CurrentVenue.getVenue();

            	 String str = v + "#" + lineArr[4].toString() ;	// # is a seperator
            	 int runs = Integer.parseInt(lineArr[7]); //without extras
        		 String r = Integer.toString(runs);
            	 pair.set(str);
            	 Runs.set(r);
        		 context.write(pair, Runs);  
             }
         }           
     }
    
    public static class MyCombiner extends Reducer<Text, Text,Text, Text>{
    	public void reduce(Text pair, Iterable<Text> values, Context context) 
                throws IOException, InterruptedException {
        	
        		//Combiner
    			Text SR = new Text();
            	String string_runs_total="";
            	int runs_total=0;
            	int balls_total=0;
            	float strikeRate = 0.000f;
            	for (Text val : values) {
              	  string_runs_total = val.toString();
              	  runs_total += Integer.parseInt(string_runs_total);
              	  balls_total += 1;
            	}            	
            	if(balls_total >= 10) {
            		strikeRate = (float)(runs_total*100/(float)balls_total);
            	}
            	//remove $ symbol
            	String sr = Float.toString(strikeRate);
            	SR.set(sr);
            	String s = pair.toString() + "#" + sr; // # is a seperator
            	pair.set(s);
            	context.write(pair,SR);
            	}
    }
    
//     Reduce function
    public static class MyReducer extends Reducer<Text, Text, Text, Text>{     
    	//final value from reducer
        public void reduce(Text pair, Iterable<Text> values, Context context) 
                throws IOException, InterruptedException {
        		//Reducer
        		String line = pair.toString();
        		String[] array = line.split("#");
        		String stadium = HighestSR.getStadium().toString();
        		String player = HighestSR.getPlayer().toString();
        		float maxSR = HighestSR.getSR();
        		//Print previous player and set new area
        		if(!stadium.equals(array[0].toString())) {
        			if(!player.isEmpty()) {
        				context.write(new Text(stadium),new Text(player));
        			}
        			HighestSR.setSR(0.0f);
        			HighestSR.setPlayer("");
        			HighestSR.setStadium(array[0].toString());
        		}
        		float currentSR = Float.parseFloat(array[2].toString());
        		if(currentSR > maxSR) {
        			HighestSR.setPlayer(array[1].toString());
        			HighestSR.setSR(currentSR);
        		}
        }
        
        protected void cleanup(Context context) throws IOException,
        	InterruptedException {
        		String stadium = HighestSR.getStadium().toString();
        		String player = HighestSR.getPlayer().toString();
        		context.write(new Text(stadium), new Text(player));
        }
    }
    
    public static class CurrentVenue {
    	static String current_venue;
    	public static void setVenue(String venue){
    		 current_venue = venue;
    	}
    	
    	public static String getVenue() {
    	   	return current_venue;
    	}
    }
    
    public static class HighestSR {
    	static String stadium = "";
    	static String player = "";
    	static float strikeRate = 0.0f;
    	
    	public static void setStadium(String s){
   		 	stadium = s;
    	}	
    	public static void setPlayer(String p){
      		player = p;
       	}	
    	public static void setSR(float sr){
    		strikeRate = sr;
    	}	
    	
	   	public static String getStadium() {
	   	   	return stadium;
	   	}
	   	public static String getPlayer() {
	   	   	return player;
	   	}
	   	public static float getSR() {
	   	   	return strikeRate;
	   	}
    }
        
    public static void main(String[] args)  throws Exception{
        Configuration conf = new Configuration();

        conf.set("mapred.textoutputformat.separator",",");
        Job job = Job.getInstance(conf, "PB");
        job.setJarByClass(BD_186_281_1583.class);
        job.setMapperClass(MyMapper.class);   
        job.setCombinerClass(MyCombiner.class);
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
	
}