import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class BD_152_554_125 {

	static HashMap<String, String> results = new HashMap<String, String>();

	public static HashMap<String, String> sortByValue(HashMap<String, String> hm) 
	{ 
		// Create a list from elements of HashMap 
		List<Map.Entry<String, String> > list = 
		       new LinkedList<Map.Entry<String, String> >(hm.entrySet()); 

		// Sort the list 
		Collections.sort(list, new Comparator<Map.Entry<String, String> >() { 
		    public int compare(Map.Entry<String, String> o1,  
				       Map.Entry<String, String> o2) 
		    { 
			String w1 = o1.getValue().split(",")[0];
			String w2 = o2.getValue().split(",")[0]; 
			String b1 = o1.getValue().split(",")[1];
			String b2 = o2.getValue().split(",")[1];
			String name1 = o1.getKey().split(",")[0];
			String name2 = o2.getKey().split(",")[0];
			String bow1 = o1.getKey().split(",")[1];
			String bow2 = o2.getKey().split(",")[1];
			if(w1.compareTo(w2) == 0) {
				if(b1.compareTo(b2) == 0) {
					if(name1.compareTo(name2) == 0)
						return bow1.compareTo(bow2);
					else
						return name1.compareTo(name2);
				}
				else if(Integer.parseInt(b1) > Integer.parseInt(b2))
					return 1;
				else
					return -1;
			}
			else
				if(Integer.parseInt(w1) < Integer.parseInt(w2))
					return 1;
				else
					return -1;
		    } 
		}); 
		  
		// put data from sorted list to hashmap  
		HashMap<String, String> temp = new LinkedHashMap<String, String>(); 
		for (Map.Entry<String, String> aa : list) { 
		    temp.put(aa.getKey(), aa.getValue()); 
		} 
		return temp; 
	}

        public static class TokenizerMapper
    extends Mapper<Object, Text, Text, Text>{

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context
                        ) throws IOException, InterruptedException {

        	String itr = value.toString();
        	String[] Record = itr.split(",");
		String val = "1";
        	if(Record[0].equals("ball"))
        	{
			int runsConceded = Integer.parseInt(Record[7]) + Integer.parseInt(Record[8]);
			context.write(new Text(Record[6] + "," + Record[4]), new Text(runsConceded + "," + val));
		}
        }
    }

    public static class IntSumCombiner
    extends Reducer<Text,Text,Text,Text> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<Text> values,
                           Context context
                           ) throws IOException, InterruptedException {
            int runs = 0;
	    int balls = 0;
            for(Text value: values)
            {
		runs += Integer.parseInt(value.toString().split(",")[0]);
            	balls += Integer.parseInt(value.toString().split(",")[1]);
            }
	    if(balls > 5)
	    {
		BD_152_554_125.results.put(key.toString(), runs + "," + balls);
            	context.write(new Text(""), new Text(runs + "," + balls));
	    }
        }
    }
    
   
    public static class IntSumReducer
    extends Reducer<Text,Text,Text,Text> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<Text> values,
                           Context context
                           ) throws IOException, InterruptedException {
            Map<String, String> result = sortByValue(BD_152_554_125.results);
            for (Map.Entry<String, String> en : result.entrySet()) { 
            	context.write(new Text(en.getKey()), new Text(en.getValue()));
            } 
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
	conf.set("mapreduce.output.textoutputformat.separator", ",");
        Job job = Job.getInstance(conf, "GasWordCount");
        job.setJarByClass(BD_152_554_125.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setMapOutputValueClass(Text.class);
        job.setCombinerClass(IntSumCombiner.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
