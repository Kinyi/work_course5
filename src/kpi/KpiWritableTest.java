package kpi;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class KpiWritableTest {

	public static void main(String[] args) throws Exception {
		final String INPUT_PATH = args[0];
		final String OUTPUT_PATH = args[1];
		Configuration conf = new Configuration();
		FileSystem fileSystem = FileSystem.get(new URI(INPUT_PATH), conf);
		if (fileSystem.exists(new Path(OUTPUT_PATH))) {
			fileSystem.delete(new Path(OUTPUT_PATH), true);
		}
		
		Job job = Job.getInstance(conf, KpiWritableTest.class.getSimpleName());
		job.setJarByClass(KpiWritableTest.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(KpiWritable.class);
		FileInputFormat.setInputPaths(job, new Path(INPUT_PATH));
		FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));
		job.waitForCompletion(true);
	}
	
	public static class MyMapper extends Mapper<LongWritable, Text, Text, KpiWritable>{
		Text k2 = new Text();
		KpiWritable v2 = new KpiWritable();
		
		@Override
		protected void map(LongWritable k1, Text v1,
				Mapper<LongWritable, Text, Text, KpiWritable>.Context context)
				throws IOException, InterruptedException {
			String[] split = v1.toString().split("\t");
			k2.set(split[1]);
			v2.set(Long.parseLong(split[6]), Long.parseLong(split[7]), Long.parseLong(split[8]), Long.parseLong(split[9]));
			context.write(k2, v2);
		}
	}
	
	public static class MyReducer extends Reducer<Text, KpiWritable, Text, KpiWritable>{
		KpiWritable v3 = new KpiWritable();
		
		@Override
		protected void reduce(Text k2, Iterable<KpiWritable> v2s,
				Reducer<Text, KpiWritable, Text, KpiWritable>.Context context)
				throws IOException, InterruptedException {
			long t1 = 0;
			long t2 = 0;
			long t3 = 0;
			long t4 = 0;
			for (KpiWritable kpiWritable : v2s) {
				t1 += kpiWritable.t1;
				t2 += kpiWritable.t2;
				t3 += kpiWritable.t3;
				t4 += kpiWritable.t4;
			}
			v3.set(t1, t2, t3, t4);
			context.write(k2, v3);
		}
	}
	
	public static class KpiWritable implements Writable{
		long t1;
		long t2;
		long t3;
		long t4;

		public KpiWritable() {
			super();
		}

		public KpiWritable(long t1, long t2, long t3, long t4) {
			super();
			set(t1, t2, t3, t4);
		}

		public void set(long t1, long t2, long t3, long t4) {
			this.t1 = t1;
			this.t2 = t2;
			this.t3 = t3;
			this.t4 = t4;
		}
		
		@Override
		public void readFields(DataInput in) throws IOException {
			this.t1 = in.readLong();
			this.t2 = in.readLong();
			this.t3 = in.readLong();
			this.t4 = in.readLong();
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeLong(t1);
			out.writeLong(t2);
			out.writeLong(t3);
			out.writeLong(t4);
		}
		
		@Override
		public String toString() {
			return t1+"\t"+t2+"\t"+t3+"\t"+t4;
		}
	}
}