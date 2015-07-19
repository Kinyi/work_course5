package realCase_mapreduce;

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

/**
 * 数据样本：5期    2015-01-26作业(论坛下载)
 * 统计联通号码1个月的主、被叫通话次数、通话时长、计费时长（按照通话次数排序，取前10行记录）
 * @author martin
 *
 */
public class RealOne1 {

	private static final String INPUT_PATH = "hdfs://122.0.67.167:8020/18681163341/sample.csv";
	private static final String OUTPUT_PATH = "hdfs://122.0.67.167:8020/18681163341/out";

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		FileSystem fileSystem = FileSystem.get(new URI(INPUT_PATH), conf);
		if (fileSystem.exists(new Path(OUTPUT_PATH))) {
			fileSystem.delete(new Path(OUTPUT_PATH), true);
		}
		Job job = Job.getInstance(conf, RealOne1.class.getSimpleName());
		job.setJarByClass(RealOne1.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NewK2.class);
		job.setNumReduceTasks(1);
		FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
		FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));
		job.waitForCompletion(true);
	}
	
	public static class MyMapper extends Mapper<LongWritable, Text, Text, NewK2>{
		Text k2 = new Text();
		NewK2 v2 = new NewK2();
		long active;
		long passive;
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, NewK2>.Context context)
				throws IOException, InterruptedException {
			String[] split = value.toString().split(",");
			String phoneNum = split[3];
			long type = Long.parseLong(split[5]);
			long callTime = Long.parseLong(split[14]);
			long chargeTime = Long.parseLong(split[15]);
			if (1 == type) {
				active = 1;
				passive = 0;
			}else if (2 == type) {
				active = 0;
				passive = 1;
			}
			k2.set(phoneNum);
			v2.set(active, passive, callTime, chargeTime);
			context.write(k2, v2);
		}
	}
	
	public static class MyReducer extends Reducer<Text, NewK2, Text, NewK2>{
		NewK2 v3 = new NewK2();
		@Override
		protected void reduce(Text k2, Iterable<NewK2> v2s,
				Reducer<Text, NewK2, Text, NewK2>.Context context)
				throws IOException, InterruptedException {
			long sumActive = 0;
			long sumPassive = 0;
			long sumCallTime = 0;
			long sumChargeTime = 0;
			for (NewK2 newK2 : v2s) {
				sumActive += newK2.active;
				sumPassive += newK2.passive;
				sumCallTime += newK2.callTime;
				sumChargeTime += newK2.chargeTime;
			}
			v3.set(sumActive, sumPassive, sumCallTime, sumChargeTime);
			context.write(k2, v3);
		}
	}
	
	public static class NewK2 implements Writable{
		long active;
		long passive;
		long callTime;
		long chargeTime;
		
		@Override
		public void write(DataOutput out) throws IOException {
			out.writeLong(active);
			out.writeLong(passive);
			out.writeLong(callTime);
			out.writeLong(chargeTime);
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			this.active = in.readLong();
			this.passive = in.readLong();
			this.callTime = in.readLong();
			this.chargeTime = in.readLong();
		}

		public void set(long active, long passive, long callTime, long chargeTime){
			this.active = active;
			this.passive = passive;
			this.callTime = callTime;
			this.chargeTime = chargeTime;
		}

		@Override
		public String toString() {
			return active + "\t" + passive	+ "\t" + callTime + "\t" + chargeTime;
		}
		
	}

}
