package inputformat;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class KeyValueTextInputFormatTest {

	private static final String INPUT_PATH = "hdfs://122.0.67.167:8020/18681163341/hello";
	private static final String OUTPUT_PATH = "hdfs://122.0.67.167:8020/18681163341/out";

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		FileSystem fileSystem = FileSystem.get(new URI(INPUT_PATH), conf);
		if (fileSystem.exists(new Path(OUTPUT_PATH))) {
			fileSystem.delete(new Path(OUTPUT_PATH), true);
		}
		
		Job job = Job.getInstance(conf, KeyValueTextInputFormatTest.class.getSimpleName());
		job.setJarByClass(KeyValueTextInputFormatTest.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.setInputPaths(job, new Path(INPUT_PATH));
		FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		job.waitForCompletion(true);
	}
	
	//当输入的格式化类为KeyValueTextInputFormat时，k1和v1的默认值都为Text
	public static class MyMapper extends Mapper<Text, Text, Text, LongWritable>{
		Text k2 = new Text();
		LongWritable v2 = new LongWritable();
		
		@Override
		protected void map(Text k1, Text v1,
				Mapper<Text, Text, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			    String[] split = v1.toString().split("\\s");
			    for (String word : split) {
					k2.set(word);
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
			for (LongWritable time : v2s) {
				sum += time.get();
			}
			v3.set(sum);
			context.write(k2, v3);
		}
	}

}
