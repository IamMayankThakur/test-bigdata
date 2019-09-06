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

public class BD_01_02 {

    public static class TokenizerMapper
    extends Mapper<Object, Text, Text, Text>{

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context
                        ) throws IOException, InterruptedException {
            String itr = value.toString();
            String[] Record = itr.split(",");
            String str = "";
            if(Record[0].charAt(0) == 'b')
            {
                if(Record[0].length() != 0 && (Record[9].charAt(0) =='b' || Record[9].charAt(0) =='c'))
                    str = "1";
                else
                    str = "0";
            }
            if(Record.length < 5)
                return;
            Text s = new Text(str);
            context.write(new Text(Record[4] + "," + Record[6]), s);
        }
    }

    public static class IntSumCombiner
    extends Reducer<Text,Text,Text,Text> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<Text> values,
                           Context context
                           ) throws IOException, InterruptedException {
            int totalWickets = 0;
            int totalDeliveries = 0;
            for(Text value: values)
            {
                String str = value.toString();
                totalDeliveries += 1;
                if(str.charAt(0) == '1')
                    totalWickets += 1;
            }
            if(totalDeliveries > 5)
            {
                Text oval = new Text(totalWickets + "," + totalDeliveries);
                context.write(key, oval);
            }
        }
    }

    public static class IntSumReducer
    extends Reducer<Text,Text,Text,Text> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Text values,
                           Context context
                           ) throws IOException, InterruptedException {
            
            context.write(key, values);

        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "BD_01_02");
        job.setJarByClass(BD_01_02.class);
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
