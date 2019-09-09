package Cricket;


import java.io.IOException;
import org.apache.hadoop.fs.Path;
//import org.apache.log4j.Logger;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

import java.util.*;

public class BowlBat{

	//private static Logger theLogger = Logger.getLogger(BowlBat.class);

	public static void main(String[] args) throws Exception {

    	Configuration conf = new Configuration();
    	//conf.set("mapreduce.output.textoutputformat.separator",",");
    	conf.set("mapred.textoutputformat.separator", ",");
		Job job = new Job(conf);
		job.setJarByClass(BowlBat.class);
		job.setJobName("Cricket");
        
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
        job.setMapperClass(Cricket.CMapper.class);
        job.setReducerClass(Cricket.CReducer.class);		

		System.exit(job.waitForCompletion(true)?0:1);
	}
}


class CMapper extends Mapper<LongWritable, Text, Text, Text> {
	private final static IntWritable one = new IntWritable(1);
	private final static IntWritable zero = new IntWritable(0);
	public String venue;
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String valueString = value.toString();
		String[] Data = valueString.split(",");
		if(Data.length>=9)
		{
		    context.write(new Text(venue+","+Data[4]), new Text(Integer.toString(Integer.parseInt(Data[7])+Integer.parseInt(Data[8]))+",1"));
		}
		else
		{
            if(Data[1].equals("venue"))
            {
                if(Data.length>3)
                    venue = Data[2]+","+Data[3];
                else
                    venue = Data[2];
                //venue = Data[2];
            }
                        
		}
	}
}
class CReducer extends Reducer<Text, Text, Text, Text> {

    public List<Float> a;
    public List<String> b;
    public List<String> c;
    public HashMap<String, String> map;
    
    @Override
    protected void setup(Context context)
    {
        a = new ArrayList<Float>();
        b = new ArrayList<String>();
        c = new ArrayList<String>();
        map = new HashMap<>(); 
    }
    //@Override
    public void reduce(Text t_key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
        Text key = t_key;
        int wicket = 0;
        int delivery = 0;
        for (Text val : values) {
            // replace type of value with the actual type of our value
            Text value = (Text) val;
            String wicdel = value.toString();
            String[] wd = wicdel.split(",");
            wicket += Integer.parseInt(wd[0]);
            delivery += Integer.parseInt(wd[1]);
            
        }
        Float wk = new Float(wicket);
        Float dl = new Float(delivery);
        String keys = (String)key.toString();
        String[] venbat = keys.split(",");
        Integer lol1 = (Integer)0;
        Integer lol2 = (Integer)0;
        String lol3;
        if(delivery>=10)
        {
            a.add(new Float(wk*100.00/dl));
            if(venbat.length > 2)
            {
                b.add(venbat[0]+','+venbat[1]);
                c.add(venbat[2]);
            }
            else
            {
                b.add(venbat[0]);
                c.add(venbat[1]);
            }
        }
    }
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException
    {
        
        Iterator i = a.listIterator();
        Iterator j = b.listIterator();
        Iterator k = c.listIterator();
        String lol1;
        String lol2;
        Float lol3;
        String temp;
        while(i.hasNext() && j.hasNext() && k.hasNext())
        {
            lol1 = (String)j.next();
            lol2 = (String)k.next();
            lol3 = (Float)i.next();
            if(map.containsKey((String)lol1))
            {
                temp = (String)map.get(lol1);
                String[] temp2 = temp.split(",");
                if(lol3 > Float.parseFloat(temp2[1]))
                {
                    map.replace(lol1,lol2+","+Float.toString(lol3));
                }
            }
            else
            {
                map.put(lol1,lol2+","+Float.toString(lol3));
            }
        }
        TreeMap<String, String> sorted = new TreeMap<>(); 
        sorted.putAll(map);
        for (Map.Entry<String, String> entry : sorted.entrySet())
        {
             String[] temp3 = ((String)entry.getValue()).split(",");
             context.write(new Text(entry.getKey()), new Text(temp3[0]));
        }
    }
    public int stringCompare(String str1, String str2) 
    { 
  
        int l1 = str1.length(); 
        int l2 = str2.length(); 
        int lmin = Math.min(l1, l2); 
  
        for (int i = 0; i < lmin; i++) { 
            int str1_ch = (int)str1.charAt(i); 
            int str2_ch = (int)str2.charAt(i); 
  
            if (str1_ch != str2_ch) { 
                return str1_ch - str2_ch; 
            } 
        } 

        if (l1 != l2) { 
            return l1 - l2; 
        } 

        else { 
            return 0; 
        } 
    } 
    
}


