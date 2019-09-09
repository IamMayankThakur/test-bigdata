import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.DataInput;
import java.io.DataOutput;

import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.fs.FSDataOutputStream;
import java.io.DataOutputStream;

public class BD_72_206_262_704 {

	public static class BowlBatWritable implements WritableComparable<BowlBatWritable> {
		private Text bowler;
		private Text batsman;

		public BowlBatWritable() {
			this.bowler = new Text();
			this.batsman = new Text();
		}

		public BowlBatWritable(Text bowlern, Text batsmann) {
			this.bowler = bowlern;
			this.batsman = batsmann;
		}

		public void set(Text bowlern, Text batsmann) {
			this.bowler = bowlern;
			this.batsman = batsmann;
		}

		public Text getBatsman() {
			return batsman;
		}

		public Text getBowler() {
			return bowler;
		}

		public void setBatsman(Text batsmann) {
			this.batsman = batsmann;
		}

		public void setBowler(Text bowlern) {
			this.bowler = bowlern;
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			batsman.readFields(in);
			bowler.readFields(in);
		}

		@Override
		public void write(DataOutput out) throws IOException {
			batsman.write(out);
			bowler.write(out);

		}

		@Override
		public String toString() {
			return this.bowler.toString() + "," + this.batsman.toString();
		}

		@Override
		public int compareTo(BowlBatWritable b) {
			int bowlcheck = bowler.compareTo(b.getBowler());
			return bowlcheck;

		}

	}

	public static class FullKeyComparator extends WritableComparator {

		public FullKeyComparator() {
			super(DeliveryWritable.class, true);
		}

		@SuppressWarnings("rawtypes")
		@Override
		public int compare(WritableComparable wc1, WritableComparable wc2) {

			DeliveryWritable key1 = (DeliveryWritable) wc1;
			DeliveryWritable key2 = (DeliveryWritable) wc2;

			if (key1.getBowlBat().compareTo(key2.getBowlBat()) == 0) {
				return 0;
			} else if (key1.getRuns().compareTo(key2.getRuns()) == 0) {
				// same runs, need to check deliveries
				if (key1.getDeliveries().compareTo(key2.getDeliveries()) == 0) {
					// another tie, go for name and call it a day
					return (key1.getBowlBat().compareTo(key2.getBowlBat()));
				}
				return key1.getDeliveries().compareTo(key2.getDeliveries());
			} else {
				return (key1.getRuns().compareTo(key2.getRuns())) * -1;
			}

		}
	}

	public static class RunBallWritable implements WritableComparable<RunBallWritable> {
		private IntWritable runs;
		private IntWritable deliveries;

		public RunBallWritable() {
			this.runs = new IntWritable();
			this.deliveries = new IntWritable();
		}

		public RunBallWritable(RunBallWritable m) {
			this.runs = new IntWritable(m.getRuns().get());
			this.deliveries = new IntWritable(m.getDeliveries().get());
		}

		public RunBallWritable(IntWritable runsn, IntWritable deliveriesn) {
			this.deliveries = deliveriesn;
			this.runs = runsn;
		}

		public void set(IntWritable runsn, IntWritable deliveriesn) {
			this.deliveries = deliveriesn;
			this.runs = runsn;
		}

		public void setRuns(IntWritable runs) {
			this.runs = runs;
		}

		public IntWritable getRuns() {
			return this.runs;
		}

		public IntWritable getDeliveries() {
			return this.deliveries;
		}

		public void setDeliveries(IntWritable deliveriesn) {
			this.deliveries = deliveriesn;
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			this.runs.readFields(in);
			this.deliveries.readFields(in);
		}

		@Override
		public void write(DataOutput out) throws IOException {
			this.runs.write(out);
			this.deliveries.write(out);
		}

		@Override
		public int compareTo(RunBallWritable w) {
			return (this.runs.compareTo(w.getRuns()));
		}

		@Override
		public String toString() {
			return this.runs.toString() + "," + this.deliveries.toString();
		}
	}

	public static class DeliveryWritable implements WritableComparable<DeliveryWritable> {
		private Text bowler;
		private Text batsman;
		private IntWritable runs;
		private IntWritable deliveries;

		public DeliveryWritable() {
			this.bowler = new Text();
			this.batsman = new Text();
			this.runs = new IntWritable();
			this.deliveries = new IntWritable();
		}

		public DeliveryWritable(Text bowler, Text batsman, IntWritable runs, IntWritable deliveries) {
			this.bowler = bowler;
			this.batsman = batsman;
			this.runs = runs;
			this.deliveries = deliveries;
		}

		public void set(Text bowler, Text batsman, IntWritable runs, IntWritable deliveries) {
			this.bowler = bowler;
			this.batsman = batsman;
			this.runs = runs;
			this.deliveries = deliveries;
		}

		public Text getBatsman() {
			return batsman;
		}

		public String getBowlBat() {
			return this.bowler.toString() + "-" + this.batsman.toString();
		}

		public Text getBowler() {
			return bowler;
		}

		public IntWritable getRuns() {
			return runs;
		}

		public IntWritable getDeliveries() {
			return deliveries;
		}

		public void setBatsman(Text batsman) {
			this.batsman = batsman;
		}

		public void setBowler(Text bowler) {
			this.bowler = bowler;
		}

		public void setRuns(IntWritable runs) {
			this.runs = runs;
		}

		public void setDeliveries(IntWritable deliveries) {
			this.deliveries = deliveries;
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			bowler.readFields(in);
			batsman.readFields(in);
			runs.readFields(in);
			deliveries.readFields(in);
		}

		@Override
		public void write(DataOutput out) throws IOException {
			bowler.write(out);
			batsman.write(out);
			runs.write(out);
			deliveries.write(out);
		}

		@Override
		public String toString() {
			return bowler.toString() + "," + batsman.toString() + "," + runs.toString() + "," + deliveries.toString();
		}

		@Override
		public int compareTo(DeliveryWritable b) {
			return this.getBowlBat().compareTo(b.getBowlBat());
		}
	}

	public static class CustomOutputFormat extends FileOutputFormat<DeliveryWritable, RunBallWritable> {
		public CustomOutputFormat() {
		};

		@Override
		public org.apache.hadoop.mapreduce.RecordWriter<DeliveryWritable, RunBallWritable> getRecordWriter(
				TaskAttemptContext arg0) throws IOException, InterruptedException {
			// get the current path
			Path path = FileOutputFormat.getOutputPath(arg0);
			// create the full path with the output directory plus our filename
			Path file = getDefaultWorkFile(arg0, "");
			// create the file in the file system
			FileSystem fs = path.getFileSystem(arg0.getConfiguration());
			FSDataOutputStream fileOut = fs.create(file, arg0);

			// create our record writer with the new file
			return new CustomRecordWriter(fileOut);
		}
	}

	public static class CustomRecordWriter extends RecordWriter<DeliveryWritable, RunBallWritable> {
		private DataOutputStream out;

		public CustomRecordWriter(DataOutputStream stream) {
			out = stream;
		}

		@Override
		public void close(TaskAttemptContext arg0) throws IOException, InterruptedException {
			// close our file
			out.close();
		}

		@Override
		public void write(DeliveryWritable arg0, RunBallWritable arg1) throws IOException, InterruptedException {
			out.writeBytes(arg0.getBowler().toString() + "," + arg0.getBatsman().toString() + ","
					+ arg1.getRuns().toString() + "," + arg1.getDeliveries().toString());

			out.writeBytes("\r\n");
		}
	}

	private static class TokenizerMapper extends Mapper<Object, Text, DeliveryWritable, RunBallWritable> {

		private Text bowler = new Text();
		private Text batsman = new Text();
		private final static IntWritable one = new IntWritable(1);
		private DeliveryWritable delw = new DeliveryWritable();
		private RunBallWritable runball = new RunBallWritable();

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
			while (itr.hasMoreTokens()) {
				String[] data = itr.nextToken().split(",", 0);
				if (data[0].equals("ball")) {
					batsman.set(data[4]);
					bowler.set(data[6]);
					IntWritable run_plus_extras = new IntWritable(
							Integer.parseInt(data[7]) + Integer.parseInt(data[8]));
					delw.set(bowler, batsman, run_plus_extras, one);
					runball.set(run_plus_extras, one);
					context.write(delw, runball);
				}
			}

		}
	}

	public static class KeyComparator implements Comparator<DeliveryWritable> {

		public int compare(DeliveryWritable key1, DeliveryWritable key2) {

			if (key1.getBowlBat().compareTo(key2.getBowlBat()) == 0) {
				return 0;
			} else if (key1.getRuns().compareTo(key2.getRuns()) == 0) {
				// same runs, need to check deliveries
				if (key1.getDeliveries().compareTo(key2.getDeliveries()) == 0) {
					// another tie, go for name and call it a day
					return (key1.getBowler().compareTo(key2.getBowler()));
				}
				return key1.getDeliveries().compareTo(key2.getDeliveries());
			} else {
				return (key1.getRuns().compareTo(key2.getRuns())) * -1;
			}

		}
	}

	public static class DeliveryReducer
			extends Reducer<DeliveryWritable, RunBallWritable, DeliveryWritable, RunBallWritable> {
		private IntWritable run_result = new IntWritable();
		private IntWritable delivery_result = new IntWritable();

		private ArrayList<DeliveryWritable> ar = new ArrayList<DeliveryWritable>();

		@Override
		public void cleanup(Context context) throws IOException, InterruptedException {
			Collections.sort(ar, new KeyComparator());
			for (int i = 0; i < ar.size(); i++) {
				context.write(ar.get(i), new RunBallWritable(ar.get(i).getRuns(), ar.get(i).getDeliveries()));
			}
		}

		@Override
		public void reduce(DeliveryWritable b, Iterable<RunBallWritable> w, Context context)
				throws IOException, InterruptedException {
			int sum_runs = 0;
			int sum_balls = 0;

			for (RunBallWritable val : w) {
				sum_runs += val.getRuns().get();
				sum_balls += val.getDeliveries().get();
			}

			run_result.set(sum_runs);
			delivery_result.set(sum_balls);

			DeliveryWritable d = new DeliveryWritable();
			RunBallWritable r = new RunBallWritable();

			if (sum_balls > 5) {
				r.set(run_result, delivery_result);
				d.set(new Text(b.getBowler().toString()), new Text(b.getBatsman().toString()),
						new IntWritable(run_result.get()), new IntWritable(delivery_result.get()));
				ar.add(d);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "BD_72_206_262_704");
		job.setJarByClass(BD_72_206_262_704.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setReducerClass(DeliveryReducer.class);

		FileSystem hdfs = FileSystem.get(conf);

		job.setOutputKeyClass(DeliveryWritable.class);
		job.setOutputValueClass(RunBallWritable.class);
		job.setOutputFormatClass(CustomOutputFormat.class);

		// Deleting output path if it exists
		if (hdfs.exists(new Path(args[1]))) {
			hdfs.delete(new Path(args[1]), true);
		}
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
