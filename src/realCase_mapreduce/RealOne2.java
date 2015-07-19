package realCase_mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 该类的作用是用于进行排序和取前10行记录
 * 
 * @author martin
 *
 */
public class RealOne2 {

	private static final String INPUT_PATH = "hdfs://122.0.67.167:8020/18681163341/out/part*";
	private static final String OUTPUT_PATH = "hdfs://122.0.67.167:8020/18681163341/out2";

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		FileSystem fileSystem = FileSystem.get(new URI(INPUT_PATH), conf);
		if (fileSystem.exists(new Path(OUTPUT_PATH))) {
			fileSystem.delete(new Path(OUTPUT_PATH), true);
		}
		Job job = Job.getInstance(conf, RealOne2.class.getSimpleName());
		job.setJarByClass(RealOne2.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		job.setMapOutputKeyClass(NewK2.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(1);
		FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
		FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));
		job.waitForCompletion(true);
	}

	public static class MyMapper extends
			Mapper<LongWritable, Text, NewK2, NullWritable> {
		NewK2 k2 = new NewK2();

		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, NewK2, NullWritable>.Context context)
				throws IOException, InterruptedException {
			String[] split = value.toString().split("\t");
			k2.set(split[0], Long.parseLong(split[1]),
					Long.parseLong(split[2]), Long.parseLong(split[3]),
					Long.parseLong(split[4]));
			context.write(k2, NullWritable.get());
		}
	}

	public static class MyReducer extends
			Reducer<NewK2, NullWritable, NewK2, NullWritable> {
		long count = 0;

		@SuppressWarnings("unused")
		@Override
		protected void reduce(
				NewK2 k2,
				Iterable<NullWritable> v2s,
				Reducer<NewK2, NullWritable, NewK2, NullWritable>.Context context)
				throws IOException, InterruptedException {
			for (NullWritable nullWritable : v2s) {
				count++;
				if (count < 11) {
					context.write(k2, NullWritable.get());
				}
			}
		}
	}

	public static class NewK2 implements WritableComparable<NewK2> {
		String phoneNum;
		long active;
		long passive;
		long callTime;
		long chargeTime;

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeUTF(phoneNum);
			out.writeLong(active);
			out.writeLong(passive);
			out.writeLong(callTime);
			out.writeLong(chargeTime);
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			this.phoneNum = in.readUTF();
			this.active = in.readLong();
			this.passive = in.readLong();
			this.callTime = in.readLong();
			this.chargeTime = in.readLong();
		}

		public void set(String phoneNum, long active, long passive,
				long callTime, long chargeTime) {
			this.phoneNum = phoneNum;
			this.active = active;
			this.passive = passive;
			this.callTime = callTime;
			this.chargeTime = chargeTime;
		}

		@Override
		public int compareTo(NewK2 o) {
			if (this.active != o.active) {
				return (int) (o.active - this.active);
			} else {
				return (int) (o.passive - this.passive);
			}
		}

		@Override
		public String toString() {
			return phoneNum + "\t" + active + "\t" + passive + "\t" + callTime
					+ "\t" + chargeTime;
		}

	}

}
