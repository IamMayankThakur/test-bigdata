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

public class BD_2362_2415_2416_2491 {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String itr = value.toString();
            String[] Record = itr.split(",");
            String val = "1";
            if (Record[0].equals("ball")) {

                if ((Record[9].equals("caught")) || (Record[9].equals("caught and bowled"))
                        || (Record[9].equals("bowled")) || (Record[9].equals("lbw")) || (Record[9].equals("stumped"))
                        || (Record[9].equals("hit wicket")) || (Record[9].equals("obstructing the field"))) {
                    context.write(new Text(Record[4] + "," + Record[6]), new Text("1:" + "1"));
                }

                else {
                    context.write(new Text(Record[4] + "," + Record[6]), new Text("1:" + "0"));
                }
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
                String deliverystr = Integer.toString(deliveries);
                if (deliverystr.length() == 1) {
                    deliverystr = "0" + "0" + deliverystr;
                } else if (deliverystr.length() == 2) {
                    deliverystr = "0" + deliverystr;
                }
                context.write(new Text("1"), new Text((999 - wickets) + "," + deliverystr + "#" + tempkey));
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
            Iterator itr = tm.keySet().iterator();
            while (itr.hasNext()) {
                String newkey = (String) itr.next();
                String[] newkeyarr = newkey.split("#");
                String thiskey = newkeyarr[1];
                String thisvalue = newkeyarr[0];
                String[] thisvaluearr = thisvalue.split(",");

                int w = 999 - Integer.parseInt(thisvaluearr[0]);
                int d = Integer.parseInt(thisvaluearr[1]);

                thisvalue = (Integer.toString(w) + "," + Integer.toString(d));
                context.write(new Text(thiskey), new Text(thisvalue));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapred.textoutputformat.separator", ",");

        Job job = Job.getInstance(conf, "BD_2362_2415_2416_2491");
        job.setJarByClass(BD_1348_1576_1597.class);
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
