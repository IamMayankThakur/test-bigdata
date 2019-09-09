import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
 
public class Batsmen {

static class SortBasedOnWic
{
	int wic;
    int run;
    String name1;
	String name2;
   
    public SortBasedOnWic(int w,int r,String n1,String n2) 
    {
        
		wic = w;	
		run = r;
		name1 = n1;
		name2 = n2;
    }
}

static class wiccompare implements Comparator<SortBasedOnWic>
{
    @Override
    public int compare(SortBasedOnWic s1, SortBasedOnWic s2)
    {
        int diff_wic = s2.wic - s1.wic;
		if(diff_wic > 0 || diff_wic < 0)		
			return diff_wic;
		else
		{
			int diff_run = s1.run-s2.run;
			if(diff_run == 0)
				return (s1.name1).compareTo(s2.name1);
			else 
				return diff_run;
		}    
	}
}

static ArrayList<SortBasedOnWic> records = new ArrayList<SortBasedOnWic>();
    


  public static class BatsmenMapper
       extends Mapper<Object, Text, Text, Text>{
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {      
      try{
	
		int is_out = 0;
		
           String[] fields = value.toString().split(",");

		
		String my_key = new Text(fields[4]) + "," + new Text(fields[6]);

		String batsmen = fields[4];
		String bowler = fields[7];
		String out_reason = fields[9];


		if(fields[9].contains("caught") || fields[9].contains("bowled") || fields[9].contains("lbw") || fields[9].contains("stumped"))
			is_out = 1;
		else
			is_out = 0;            
		
		 String my_value = String.valueOf(is_out) + ","+ String.valueOf(1);

		context.write(new Text(my_key) , new Text(my_value));

      }catch(Exception e){
       }
    }
  }


  public static class BatsmenReducer
       extends Reducer<Text,Text,Text,Text> {

    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {

	int sum_match = 0;
	int sum_out = 0;

       for (Text val : values) 
	{
	  String line = val.toString();
        String[] field = line.split(",");
	  sum_out += Integer.parseInt(field[0]);	
	  sum_match += 1;
	
	 }

	String my_val = String.valueOf(sum_out) + "," +String.valueOf(sum_match);
	
	String line1 = key.toString();
      String[] field1 = line1.split(",");
	String readname1 = field1[0];	
	String readname2 = field1[1];
	int readwic = sum_out;             
      int readrun = sum_match;
         	
	if(sum_match > 5)	
	 	records.add(new SortBasedOnWic(readwic,readrun,readname1,readname2));	

	 }	


	@Override
	public void cleanup(Context context) throws IOException, 
									InterruptedException 
	{
	
	
	Collections.sort(records, new wiccompare());
 
		 for (SortBasedOnWic res : records) 
      	  {
			
			//System.out.println(res.name1+","+res.name2+","+res.wic+","+res.run);
		 	context.write(new Text(res.name1+","+res.name2) , new Text(res.wic+","+res.run) );       
		}

		
  	}

}


  public static void main(String[] args) throws Exception {
	Path out = new Path(args[1]);

    Configuration conf = new Configuration();
	conf.set("mapred.textoutputformat.separator", ","); 

    Job job = Job.getInstance(conf, "Batsmen");
    job.setJarByClass(Batsmen.class);
    job.setMapperClass(BatsmenMapper.class);
    job.setReducerClass(BatsmenReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);


	
	FileInputFormat.addInputPath(job, new Path(args[0]));
	//FileOutputFormat.setOutputPath(job, new Path(out, "out_Task2"));
	
	FileOutputFormat.setOutputPath(job, out);	
	job.waitForCompletion(true);
	
	// BufferedWriter writer = new BufferedWriter(new FileWriter("task2_new.txt"));
	
	
  }
}












