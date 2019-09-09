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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

public class BD_0980_1528_1600 {



  public static class venueMapper
       extends Mapper<Object, Text, Text, Text>{
	String myVenue=new String();

	           
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {      
      try{
			String[] fields = value.toString().split(",");


		if(fields[0].equals("info") && fields[1].equals("venue"))
			{
				if(fields[2].startsWith("\""))
					{
					myVenue = fields[2]  + "," + fields[3];
					}				
				else
					myVenue = fields[2];	

			}		

		int extras = Integer.parseInt(fields[8]);
		if(fields[0].equals("ball") && (extras == 0))           
		{
			String myValue = String.valueOf(fields[7]) + "," +String.valueOf(1);	
			context.write(new Text(myVenue+":"+fields[4]) , new Text(myValue));
			
		}
		

		
      }catch(Exception e){
        
       }
    }
  }



  public static class venueReducer
       extends Reducer<Text,Text,Text,Text> {

    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {

float sum_runs = 0;
float sum_del = 0;

for(Text val : values)
		{
			String line = val.toString();
        		String[] field = line.split(",");
			sum_runs += Float.parseFloat(field[0]);
			sum_del += 1.0;		
		}

	float stRate = (((sum_runs)/(sum_del)) * 100);
	String[] arr = (key.toString()).split(":");	
	String myKey = arr[0];	
	int sum_runs_int = (int)sum_runs;
	String myValue =  arr[1] + "," + String.valueOf(stRate)+","+String.valueOf(sum_runs_int);
	
	if(sum_del >= 10)	
		context.write(new Text(myKey) , new Text(myValue));
	}
}

 public static class venueReducer1
       extends Reducer<Text,Text,Text,Text> {

    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
		float max_strate = 0;
		int max_runs = 0;
		String max_bat = "";
		for (Text val : values)
			{
			String line = val.toString();
		    	String[] field = line.split(",");
			Float strate = Float.parseFloat(field[1]);
			Integer runs = Integer.parseInt(field[2]);
			if(strate >= max_strate)	
				{
				max_strate= strate;
				max_bat = field[0];		
				max_runs = runs;				
				}
			
			}
		String ven = key.toString();
		int max_str = (int)max_strate;
		//records.add(new SortBasedOnWic(max_str,max_runs,ven,max_bat));	
		
		context.write(key, new Text(max_bat));
	 	
	}
}

  public static void main(String[] args) throws Exception {
	Path out = new Path(args[1]);

    Configuration conf = new Configuration();
conf.set("mapred.textoutputformat.separator", ","); 

    Job job = Job.getInstance(conf, "venue");
    job.setJarByClass(BD_0980_1528_1600.class);
    job.setMapperClass(venueMapper.class);
    
    job.setCombinerClass(venueReducer.class);
    job.setReducerClass(venueReducer1.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

	FileInputFormat.addInputPath(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job, out);
	job.waitForCompletion(true);
	

  }
}


/*


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
	return (s1.name1).compareTo(s2.name1);
    }
}

static ArrayList<SortBasedOnWic> records = new ArrayList<SortBasedOnWic>();



		Collections.sort(records, new wiccompare());

	 BufferedWriter writer = new BufferedWriter(new FileWriter("task41.txt"));
 	for (SortBasedOnWic res : records) 
        {
			   
			writer.write(res.name1);
			writer.write(","+res.name2);         
			writer.write(","+res.wic);
			writer.write(","+res.run);

			
          writer.newLine();
        }
	 writer.close();	
	
*/











