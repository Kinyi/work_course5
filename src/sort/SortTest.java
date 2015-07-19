package sort;

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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SortTest {
	private static final String INPUT_PATH = "hdfs://122.0.67.167:8020/18681163341/twocolumns";
	private static final String OUTPUT_PATH = "hdfs://122.0.67.167:8020/18681163341/out";

	/**
	 * 3	3
	 * 3	2
	 * 3	1
	 * 2	2
	 * 2	1
	 * 1	1
	 * 两列按从小到大排列
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		FileSystem fileSystem = FileSystem.get(new URI(INPUT_PATH), conf);
		if (fileSystem.exists(new Path(OUTPUT_PATH))) {
			fileSystem.delete(new Path(OUTPUT_PATH), true);
		}
		
		Job job = Job.getInstance(conf, SortTest.class.getSimpleName());
		job.setJarByClass(SortTest.class);
		job.setMapperClass(MyMapper.class);
		FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
		FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));
		job.setNumReduceTasks(1);
		job.setMapOutputKeyClass(TwoColumns.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.waitForCompletion(true);
	}
	
	public static class TwoColumns implements WritableComparable<TwoColumns>{
		int first;
		int second;

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeInt(first);
			out.writeInt(second);
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			this.first = in.readInt();
			this.second = in.readInt();
		}

		@Override
		public int compareTo(TwoColumns o) {
			if (this.first != o.first) {
				return this.first - o.first;
			}else {
				return this.second - o.second;
			}
		}

		public void set(String first ,String second) {
			this.first = Integer.parseInt(first);
			this.second = Integer.parseInt(second);
		}

		@Override
		public String toString() {
			return first + "\t" + second;
		}
	}
	
	public static class MyMapper extends Mapper<LongWritable, Text, TwoColumns, NullWritable>{
		TwoColumns k2 = new TwoColumns();
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, TwoColumns, NullWritable>.Context context)
				throws IOException, InterruptedException {
			String[] split = value.toString().split("\t");
			k2.set(split[0], split[1]);
			context.write(k2, NullWritable.get());
		}
	}

}
