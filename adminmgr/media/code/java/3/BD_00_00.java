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

public class BD_00_00 {

    public static class TokenizerMapper
    extends Mapper<Object, Text, Text, Text>{

        static String ven = "";
        public void map(Object key, Text value, Context context
                        ) throws IOException, InterruptedException {
            String itr = value.toString();
            String[] Record = itr.split(",");
            if(Record[1].equals("venue"))
            {
                ven = Record[2];
                // if(ven.equals("M.Chinnaswamy Stadium"))
                // {
                //     ven = "M Chinnaswamy Stadium";
                // }
                // if(ven.equals("\"Punjab Cricket Association IS Bindra Stadium"))
                // {
                //     ven = "\"Punjab Cricket Association Stadium";
                // }
                if(Record.length > 3)
                {
                    ven = ven + "," + Record[3];
                }
                if(ven.charAt(0) == '"')
                {
                    ven = ven.substring(1,ven.length()-1);
                }
            }
            if(Record[0].equals("ball") && Record[8].equals("0"))
            {
                context.write(new Text(ven  + "\t" + Record[4]), new Text(Record[7]));
            }
        }
    }

    public static class IntSumCombiner
    extends Reducer<Text,Text,Text,Text> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<Text> values,
                           Context context
                           ) throws IOException, InterruptedException {
            int totalRuns = 0;
            int totalDeliveries = 0;
            int sr = 0;
            String k;
            for(Text value: values)
            {
                String str = value.toString();
                totalDeliveries += 1;
                totalRuns += Integer.parseInt(str);
            }
            if(totalDeliveries >= 10)
            {
                k = key.toString();
                String rec[] = k.split("\t");
                sr = (totalRuns * 100) / totalDeliveries;
                context.write(new Text(rec[0]), new Text(rec[1]+ "," + sr + "," + totalRuns));
            }
        }
    }

    public static class IntSumReducer
    extends Reducer<Text,Text,Text,Text> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<Text> values,
                           Context context
                           ) throws IOException, InterruptedException {
            String ven = key.toString();
            int runs, sr;
            String mpb = "";
            int mr = 0, msr = 0;
            for(Text value: values)
            {
                String str = value.toString();
                String[] rec = str.split(",");
                runs = Integer.parseInt(rec[2]);
                sr = Integer.parseInt(rec[1]);
                if(sr > msr)
                {
                    mpb = rec[0];
                    msr = sr;
                    mr = runs;
                }
                if(sr == msr)
                    if(runs > mr)
                    {
                        mpb = rec[0];
                        msr = sr;
                        mr = runs;
                    }
            }
            String temp=key.toString();
            temp+=",";
            context.write(new Text(temp),new Text(mpb));

        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "BD_00_00");
        job.setJarByClass(BD_00_00.class);
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
