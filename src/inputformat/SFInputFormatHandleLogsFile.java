package inputformat;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 在SequenceFileTest2中，对hadoop下面的logs文件夹中的内容，使用SequenceFile格式存储到HDFS中。请对该SequenceFile文件进行单词计数
 * @author martin
 *
 */
public class SFInputFormatHandleLogsFile {
	private static final String INPUT_PATH = "hdfs://crxy2:9000/sf_logs";
	private static final String OUTPUT_PATH = "hdfs://crxy2:9000/out";

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		FileSystem fileSystem = FileSystem.get(new URI(INPUT_PATH), conf);
		if (fileSystem.exists(new Path(OUTPUT_PATH))) {
			fileSystem.delete(new Path(OUTPUT_PATH), true);
		}
		Job job = Job.getInstance(conf, SFInputFormatHandleLogsFile.class.getSimpleName());
		job.setJarByClass(SFInputFormatHandleLogsFile.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
		FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));
		job.waitForCompletion(true);
	}
	
	public static class MyMapper extends Mapper<Text, BytesWritable, Text, LongWritable>{
		Text k2 = new Text();
		LongWritable v2 = new LongWritable();
		@Override
		protected void map(Text key, BytesWritable value,
				Mapper<Text, BytesWritable, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			String[] split = new String(value.getBytes()).split("[\\s,./;:=<>]");
			for (String string : split) {
				k2.set(string);
				v2.set(1);
				context.write(k2, v2);
			}
		}
	}
	
	public static class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable>{
		LongWritable v3 = new LongWritable();
		@Override
		protected void reduce(Text k2, Iterable<LongWritable> v2s,
				Reducer<Text, LongWritable, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			long sum = 0;
			for (LongWritable longWritable : v2s) {
				sum += longWritable.get();
			}
			v3.set(sum);
			context.write(k2, v3);
		}
	}
}
