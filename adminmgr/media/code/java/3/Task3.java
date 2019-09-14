import java.io.IOException;
import java.lang.Integer;
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

public class BD_444_459_489 {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {

        private final static IntWritable one = new IntWritable(1);
        public String venue = new String();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String itr = value.toString();
            String[] Record = itr.split(",");
            String val = "1";

            if (Record[0].equals("info") && Record[1].equals("venue")) {
                if ((Record[2].equals("\"MA Chidambaram Stadium"))
                        || (Record[2].equals("\"Vidarbha Cricket Association Stadium"))
                        || (Record[2].equals("\"Punjab Cricket Association IS Bindra Stadium"))
                        || (Record[2].equals("\"Punjab Cricket Association Stadium"))
                        || (Record[2].equals("\"Rajiv Gandhi International Stadium"))
                        || (Record[2].equals("\"Sardar Patel Stadium"))) {
                    venue = Record[2] + "," + Record[3];
                } else {
                    venue = Record[2];
                }
            }
            if (Record[0].equals("ball")) {
                if (Record[8].equals("0")) {
                    context.write(new Text(venue + ":" + Record[4]), new Text(Record[7]));
                }
            }
        }
    }

    public static class IntSumCombiner extends Reducer<Text, Text, Text, Text> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int deliveries = 0;
            int runs = 0;
            int wickets = 0;
            int tempstr;
            int count = 0;
            double strikerate;
            for (Text value : values) {
                tempstr = Integer.parseInt(value.toString());
                runs += tempstr;
                count += 1;

            }
            String[] keysplit = (key.toString()).split(":");
            String newkey = keysplit[0];
            double runsf = runs;
            double countf = count;

            strikerate = (runsf / countf) * 100;
            String tempruns = Double.toString(strikerate);
            if (count >= 10) {
                context.write(new Text(newkey), new Text("" + keysplit[1] + ":" + tempruns + ":" + runsf));
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double maxruns = 0.0;
            double maxrunsf = 0.0;
            String maxname = "";
            for (Text value : values) {
                String temp = value.toString();
                String[] temparr = temp.split(":");
                if (Double.parseDouble(temparr[1]) == maxruns) {

                    if (Double.parseDouble(temparr[2]) > maxrunsf) {
                        maxruns = Double.parseDouble(temparr[1]);
                        maxrunsf = Double.parseDouble(temparr[2]);
                        maxname = temparr[0];
                    }

                } else if (Double.parseDouble(temparr[1]) > maxruns) {
                    maxruns = Double.parseDouble(temparr[1]);
                    maxrunsf = Double.parseDouble(temparr[2]);
                    maxname = temparr[0];
                }
            }
            if ((key.toString()).equals("")) {
            } else {
                context.write(key, new Text(maxname));

            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapred.textoutputformat.separator", ",");
        Job job = Job.getInstance(conf, "BD_444_459_489");
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
