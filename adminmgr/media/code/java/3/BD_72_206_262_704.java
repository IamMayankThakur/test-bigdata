import java.io.IOException;

import java.io.DataInput;

import java.io.DataOutput;

import java.util.StringTokenizer;

import java.util.Map;

import java.util.HashMap;

import java.util.Iterator;



import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.io.MapWritable;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.io.Writable;

import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class BD_72_206_262_704 {

    public static class MyMapWritable 
        extends MapWritable {
    
        @Override
        public String toString() { 

            String result="";
            for (Object key : this.keySet()) {
                result+=key.toString();
            }
            return result;
        } 
    
    }

	public static class RunDelWritable 

		implements Writable {

		private IntWritable runs;

		private IntWritable dels;

		public RunDelWritable() {

			this.runs=new IntWritable();

			this.dels=new IntWritable();

		}

		public RunDelWritable(IntWritable nruns, IntWritable ndels) {

			this.runs=nruns;

			this.dels=ndels;

		}

		public void set(IntWritable nruns, IntWritable ndels) {

			this.runs=nruns;

			this.dels=ndels;

		}

		public IntWritable getRuns() {

			return runs;

		}

		public IntWritable getDels() {

			return dels;

		}

		public void readFields(DataInput in) throws IOException {

			runs.readFields(in);

			dels.readFields(in);

		}

		public void write(DataOutput out) throws IOException {

			runs.write(out);

			dels.write(out);

		}

	}



	public static class MatchMapper 

		extends Mapper<Object, Text, Text, MyMapWritable> {

        private Text venue=new Text();

        public void map(Object key, Text value, Context context
            ) throws IOException, InterruptedException {

			String[] data=value.toString().split(",",0);
			if(data[0].equals("info") && data[1].equals("venue")) {
				venue.set(value.toString().split(",",3)[2]);	//handle venue name containing commas
			}
			else if (data[0].equals("ball") && Integer.parseInt(data[8])==0) {	//only if extra runs are 0, ie, no delivery considered otherwise
			    MyMapWritable batsmen=new MyMapWritable();
				batsmen.put( new Text(data[4]) , 
					new RunDelWritable( new IntWritable(Integer.parseInt(data[7])) , new IntWritable(1) ) );
				context.write(venue,batsmen);
			}
        }

    }



    public static class VenueCombiner 

		extends Reducer<Text,MyMapWritable,Text,MyMapWritable> {


		public void reduce(Text key, Iterable<MyMapWritable> values, Context context

            ) throws IOException, InterruptedException {

			
            MyMapWritable batsmen=new MyMapWritable();
			RunDelWritable otemp,ntemp;

        	for (MyMapWritable match : values) {	//for each match in venue key

				for (Writable b : match.keySet()) {	//for each batsman in match

					Text batsman=(Text)b;

					if (!batsmen.containsKey(batsman)) {	//if new batsman

						batsmen.put(batsman,match.get(batsman));

					}

					else {	//if already added batsman

						otemp=(RunDelWritable)batsmen.get(batsman);

						ntemp=(RunDelWritable)match.get(batsman);

						otemp.set(new IntWritable( otemp.getRuns().get() + ntemp.getRuns().get() ), 

							new IntWritable( otemp.getDels().get() + ntemp.getDels().get() ) );

						batsmen.put(batsman,otemp);

					}

				}

			}

			context.write(key,batsmen);

        }

    }

   

    public static class ProlificReducer

    	extends Reducer<Text,MyMapWritable,Text,MyMapWritable> {


        public void reduce(Text key, Iterable<MyMapWritable> values, Context context

            ) throws IOException, InterruptedException {

			
			float strrate;

			RunDelWritable temp;

			String prolific=new String();
			Map<String,Float> strrates= new HashMap<String,Float>();

			float beststrrate=0.0f;
			
			for(MyMapWritable batsmen : values) { //only 1 iteration

                for (Writable b : batsmen.keySet()) {	//only 1 key from each venue because matches already reduced in combiner

				    Text batsman=(Text)b;

				    temp=(RunDelWritable)batsmen.get(batsman);

				    if (temp.getDels().get()>=10) {	//consider only if batsman faced at least 10 deliveries

					    strrate=( (float)temp.getRuns().get() * 100) / temp.getDels().get();

					    strrates.put(batsman.toString(),strrate);

				    }

			    }

			    for (String batsman : strrates.keySet()) {  //prolific batsman calc

				    if( (strrates.get(batsman) - beststrrate) > 0.0001f) {	//float values are stored approximately, so difference should be greater than an acceptable error value

					    prolific=batsman;

					    beststrrate=strrates.get(batsman);

				    }

				    else if (Math.abs(strrates.get(batsman)-beststrrate)<0.0001f) {	//if nearly equal, test with total runs

					    if ( ((RunDelWritable)batsmen.get(new Text(batsman))).getRuns().get() > 

						    ((RunDelWritable)batsmen.get(new Text(prolific))).getRuns().get() ) {

							    prolific=batsman;

							    beststrrate=strrates.get(batsman);

						    }

				    }

			    }
            }
            
            MyMapWritable mostProlific=new MyMapWritable();
            mostProlific.put( new Text(prolific) , new Text() );
			context.write( new Text( key.toString() ) , mostProlific );

        }

    }





    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.set("mapreduce.output.textoutputformat.separator", ",");

        Job job = Job.getInstance(conf, "BD_72_206_262_704");

        job.setJarByClass(BD_72_206_262_704.class);

        job.setMapperClass(MatchMapper.class);

		job.setCombinerClass(VenueCombiner.class);

		job.setReducerClass(ProlificReducer.class);

        job.setOutputKeyClass(Text.class);

        job.setOutputValueClass(MyMapWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));

        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

}
