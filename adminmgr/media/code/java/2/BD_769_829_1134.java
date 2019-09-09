import java.io.IOException;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
public class BD_769_829_1134 {
    static class Task3Mapper
    extends Mapper<LongWritable, Text , Text, IntWritable>{

public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    String[] values= value.toString().split(",");
    IntWritable runs=new IntWritable(0);
		if(values[0].equals("ball")) {
        String keys = values[6]+","+values[4];
        runs.set(Integer.parseInt(values[8])+Integer.parseInt(values[7]));
		context.write(new Text(keys), runs);
        }
		
    }
}
    static class Task3Reducer
    extends Reducer<Text,IntWritable, Text, Text> {
    	public Map<String,Integer> map=new LinkedHashMap<String,Integer>();
    	public void reduce(Text key, Iterable<IntWritable> values,
                   Context context) throws IOException, InterruptedException {
    		int ballCount = 0;
		    int runCount=0;
		    for (IntWritable value : values) {
		            	runCount += value.get(); 
		            	ballCount++;
		 }
			if(ballCount>5)
			{	
				String runCount_BallCount= Integer.toString(runCount) + "," + Integer.toString(ballCount);
				String alldata=key.toString()+","+runCount_BallCount;
				map.put(alldata,new Integer(runCount));
			}
}
    	public void cleanup(Context context) throws IOException,InterruptedException
		{
			List<Map.Entry<String,Integer>> list=new ArrayList<>(map.entrySet());
			Collections.sort(list, new Comparator<Map.Entry<String, Integer>>(){
	            @Override
	            public int compare(Map.Entry<String, Integer> d1, Map.Entry<String, Integer> d2){
	                int runcount = d2.getValue().compareTo(d1.getValue());
	                if (runcount != 0) {
	                    return runcount;
	                }
	                Integer ballsBowled_a=Integer.parseInt(d1.getKey().split(",")[3]);
	                Integer ballsBowled_b=Integer.parseInt(d2.getKey().split(",")[3]);
	                String bow_a=d1.getKey().split(",")[0];
	                String bow_b=d2.getKey().split(",")[0];
	                
	                
	                int bBowled = ballsBowled_a.compareTo(ballsBowled_b);
	                if (bBowled != 0) {
	                    return bBowled;
	                }
	               return bow_a.compareTo(bow_b);
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
        job.setMapperClass(Task3Mapper.class);
	
        job.setReducerClass(Task3Reducer.class);
	job.setMapOutputKeyClass(Text.class);
	job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0:1);

    }

}
