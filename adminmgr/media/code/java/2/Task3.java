import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Task3 {

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
if(Record[8].equals("0")) {


//if((Record[9].equals("caught")) || (Record[9].equals("caught and bowled")) || (Record[9].equals("bowled")) || (Record[9].equals("lbw")))  {
        // context.write(new Text(Record[6]+":" + Record[4]), new Text("1:"+"1"));
        //}
//else
context.write(new Text(Record[6]+":" + Record[4]), new Text("1:"+Record[7]));
}
}
        }
    }

   /* public static class IntSumCombiner
    extends Reducer<Text,Text,Text,Text> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<Text> values,
                           Context context
                           ) throws IOException, InterruptedException {
        int count = 0;
        for(Text value: values)
        {
        count ++;
        }
        context.write(key, new Text(""+count));
        }
    }
   */
    public static class IntSumReducer
    extends Reducer<Text,Text,Text,Text> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<Text> values,
                           Context context
                           ) throws IOException, InterruptedException {
            int deliveries = 0;
   int runs = 0;
   String tempstr;String[] temp;
   int tempint = 0;
            for(Text value: values)
            { tempstr = value.toString();
temp = tempstr.split(":");
tempint = Integer.parseInt(temp[1].toString());
//String[] temp = tempstr.split(":");
runs += tempint;
deliveries += 1;

           
            }context.write(key, new Text(""+runs+", "+deliveries));
    }
}


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Task3");
        job.setJarByClass(Task3.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setMapOutputValueClass(Text.class);
        //job.setCombinerClass(IntSumCombiner.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
