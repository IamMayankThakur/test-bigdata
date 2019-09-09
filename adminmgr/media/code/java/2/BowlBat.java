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
		job.setOutputValueClass(Cricket.IntArrayWritable.class);
		
        job.setMapperClass(Cricket.CMapper.class);
        job.setReducerClass(Cricket.CReducer.class);		

		System.exit(job.waitForCompletion(true)?0:1);
	}
}


class CMapper extends Mapper<LongWritable, Text, Text, Text> {
	private final static IntWritable one = new IntWritable(1);
	private final static IntWritable zero = new IntWritable(0);
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String valueString = value.toString();
		String[] Data = valueString.split(",");
		if(Data.length>=9)
		{
		    context.write(new Text(Data[6]+","+Data[4]), new Text(Integer.toString(Integer.parseInt(Data[7])+Integer.parseInt(Data[8]))+",1"));
		}
	}
}

class CReducer extends Reducer<Text, Text, Text, IntArrayWritable> {

    public List<Integer> a;
    public List<Integer> b;
    public List<String> c;
    
    @Override
    protected void setup(Context context)
    {
        a = new ArrayList<Integer>();
        b = new ArrayList<Integer>();
        c = new ArrayList<String>();
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
        String wk = Integer.toString(wicket);
        String dl = Integer.toString(delivery);
        String keys = (String)key.toString();
        Integer lol1 = (Integer)0;
        Integer lol2 = (Integer)0;
        String lol3;
        if(delivery>5)
        {
            Iterator i = a.listIterator();
            Iterator k = b.listIterator();
            Iterator l = c.listIterator();
            int j = 0;
            int n = a.size();
            boolean flag = false;
            if(n>0)
            {
                while(i.hasNext())
                {
                    lol1 = (Integer)i.next();
                    if(lol1 == (Integer)wicket)
                    {
                        k = b.listIterator(j);
                        lol2 = (Integer)k.next();
                        if(lol2 == (Integer)delivery)
                        {
                            l = c.listIterator(j);
                            lol3 = (String)l.next();
                            if(stringCompare(keys, lol3)>0)
                                break;
                        }
                        else if(lol2 < (Integer)delivery)
                            break;
                    }
                    else if(lol1 < (Integer)wicket)
                        break;
                    j = j + 1;
                }
                
            }
            if(j==n)
            {
                a.add(wicket);
                b.add(delivery);
                c.add(keys);
            }
            else
            {
                a.add(j,wicket);
                b.add(j,delivery);
                c.add(j,keys);
            }
            
        }
    }
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException
    {
        
        Iterator i = a.listIterator();
        Iterator j = b.listIterator();
        Iterator k = c.listIterator();
        while(i.hasNext() && j.hasNext() && k.hasNext())
        {
            IntWritable[] temp = new IntWritable[2];
            temp[0] = new IntWritable((Integer)i.next());
            temp[1] = new IntWritable((Integer)j.next());
            context.write(new Text(((String)k.next())),new IntArrayWritable(temp));
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
class IntArrayWritable extends ArrayWritable {

    public IntArrayWritable(IntWritable[] values) {
        super(IntWritable.class, values);
    }

    @Override
    public IntWritable[] get() {
        return (IntWritable[]) super.get();
    }

    @Override
    public String toString() {
        IntWritable[] values = get();
        return values[0].toString() + "," + values[1].toString();
    }
}
