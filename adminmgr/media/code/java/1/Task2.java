import java.io.*;
import java.util.*;
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

import java.util.Map;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.Iterator;

public class Task2 {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String itr = value.toString();
            String[] Record = itr.split(",");
            String val = "1";
            if (Record[0].equals("ball")) {

                if ((Record[9].equals("caught")) || (Record[9].equals("caught and bowled"))
                        || (Record[9].equals("bowled")) || (Record[9].equals("lbw"))) {
                    context.write(new Text(Record[4] + "," + Record[6]), new Text("1:" + "1"));
                } else
                    context.write(new Text(Record[4] + "," + Record[6]), new Text("1:" + "0"));
            }
        }
    }

    public static class IntSumCombiner extends Reducer<Text, Text, Text, Text> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int deliveries = 0;
            int wickets = 0;
            String tempstr;
            for (Text value : values) {
                tempstr = value.toString();
                String[] temp = tempstr.split(":");
                if (temp[1].equals("1")) {
                    wickets += 1;
                }
                deliveries += 1;
            }
            if (deliveries > 5) {
                String tempkey = key.toString();
                context.write(new Text("1"), new Text(wickets + "," + (10000 - deliveries) + ":" + tempkey));
            }
        }

    }

    public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {

        private Map countMap = new HashMap<>();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text val : values) {
                countMap.put(val.toString(), "1");
            }
            TreeMap<String, String> tm = new TreeMap<String, String>(countMap);
            Iterator itr = tm.descendingKeySet().iterator();

            while (itr.hasNext()) {
                String newkey = (String) itr.next();
                String[] newkeyarr = newkey.split(":");
                String thiskey = newkeyarr[1];
                String thisvalue = newkeyarr[0];
                String[] thisvaluearr = thisvalue.split(",");
                int deliveries = 10000 - Integer.parseInt(thisvaluearr[1]);
                thisvalue = thisvaluearr[0] + "," + Integer.toString(deliveries);
                context.write(new Text(thiskey), new Text(thisvalue));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        // conf.set("mapred.textoutputformat.seperator", ",");
        conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", ",");
        Job job = Job.getInstance(conf, "Task2");
        job.setJarByClass(Task2.class);
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