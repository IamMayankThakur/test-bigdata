import java.io.IOException;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;

public class BD_769_829_1134{
	static class Task2Mapper
    extends Mapper<LongWritable, Text , Text, IntWritable>{
		
public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	IntWritable wicket=new IntWritable(0);
    String[] values= value.toString().split(",");
		if(values[0].equals("ball")) {
        String keys= values[4]+","+values[6];
        wicket.set(0);
		if((values[10].charAt(0)>= 97 && values[10].charAt(0) <=122 ||  values[10].charAt(0)>= 65 && values[10].charAt(0)<= 90 ) && !(values[9].equals("run out"))&&!(values[9].equals("retired hurt")))
			wicket.set(1);
		context.write(new Text(keys),wicket);
        }
    }
}

static class Task2Reducer
    extends Reducer<Text,IntWritable, Text, NullWritable> {
		public Map<String,Integer> map=new LinkedHashMap<String,Integer>();

		public void reduce(Text key, Iterable<IntWritable> values,
		                   Context context) throws IOException, InterruptedException {
		
		     int ballCount = 0;
		    int wicketCount = 0;
		    for (IntWritable value : values) {
		            	wicketCount += value.get(); 
		            	ballCount++;
		 }
			if(ballCount>5)
			{	
				String ballcount_wicketCount= Integer.toString(wicketCount) + "," + Integer.toString(ballCount);
				String alldata=key.toString()+","+ballcount_wicketCount;
				map.put(alldata,new Integer(wicketCount));
			}
			
		
		}
		
		public void cleanup(Context context) throws IOException,InterruptedException
		{
			
			List<Map.Entry<String,Integer>> list=new ArrayList<>(map.entrySet());
			Collections.sort(list, new Comparator<Map.Entry<String, Integer>>(){
	            @Override
	            public int compare(Map.Entry<String, Integer> d1, Map.Entry<String, Integer> d2){
	                int wicketcount = d2.getValue().compareTo(d1.getValue());
	                if (wicketcount != 0) {
	                    return wicketcount;
	                }
	                Integer ballsFacedBy_a=Integer.parseInt(d1.getKey().split(",")[3]);
	                Integer ballsFacedBy_b=Integer.parseInt(d2.getKey().split(",")[3]);
	                String bat_a=d1.getKey().split(",")[0];
	                String bat_b=d2.getKey().split(",")[0];
	                
	                
	                int bfaced = ballsFacedBy_a.compareTo(ballsFacedBy_b);
	                if (bfaced != 0) {
	                    return bfaced;
	                }
	               return bat_a.compareTo(bat_b);
	            }});					
			for(Map.Entry<String, Integer> entry:list) {

				context.write(new Text(entry.getKey()),null);
			}
		}
}

public static void main(String[] args) throws Exception {
Configuration conf = new Configuration();
Job job = Job.getInstance(conf, "ipl");
job.setJarByClass(BD_769_829_1134.class);
job.setMapperClass(Task2Mapper.class);
job.setReducerClass(Task2Reducer.class);
job.setMapOutputKeyClass(Text.class);
job.setMapOutputValueClass(IntWritable.class);
job.setOutputKeyClass(IntWritable.class);
job.setOutputValueClass(NullWritable.class);

FileInputFormat.addInputPath(job, new Path(args[0]));
FileOutputFormat.setOutputPath(job, new Path(args[1]));

System.exit(job.waitForCompletion(true) ? 0:1);

}
	

}
