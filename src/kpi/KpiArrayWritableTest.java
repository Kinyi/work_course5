package kpi;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 对于电信流量汇总的例子，要求<k2,v2>类型分别是Text,ArrayWritable；<k3,v3>类型分别是Text，NullWritable实现
 */

//method one

/*public class KpiArrayWritableTest {

	public static void main(String[] args) throws Exception {
		final String INPUT_PATH = args[0];
		final String OUTPUT_PATH = args[1];
		Configuration conf = new Configuration();
		FileSystem fileSystem = FileSystem.get(new URI(INPUT_PATH), conf);
		if (fileSystem.exists(new Path(OUTPUT_PATH))) {
			fileSystem.delete(new Path(OUTPUT_PATH), true);
		}

		Job job = Job.getInstance(conf,
				KpiArrayWritableTest.class.getSimpleName());
		job.setJarByClass(KpiArrayWritableTest.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(TextArrayWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		FileInputFormat.setInputPaths(job, new Path(INPUT_PATH));
		FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));
		job.waitForCompletion(true);
	}

	public static class MyMapper extends
			Mapper<LongWritable, Text, Text, TextArrayWritable> {
		Text k2 = new Text();

		@Override
		protected void map(
				LongWritable k1,
				Text v1,
				Mapper<LongWritable, Text, Text, TextArrayWritable>.Context context)
				throws IOException, InterruptedException {
			String[] split = v1.toString().split("\t");
			k2.set(split[1]);
			String[] strings = new String[] { split[6], split[7], split[8],
					split[9] };
			TextArrayWritable v2 = new TextArrayWritable(strings);
			context.write(k2, v2);
		}
	}

	public static class MyReducer extends
			Reducer<Text, TextArrayWritable, Text, NullWritable> {
		Text k3 = new Text();

		@Override
		protected void reduce(
				Text k2,
				Iterable<TextArrayWritable> v2s,
				Reducer<Text, TextArrayWritable, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			long t0 = 0;
			long t1 = 0;
			long t2 = 0;
			long t3 = 0;
			for (TextArrayWritable v2 : v2s) {
				String[] strings = v2.toStrings();
				t0 += Long.parseLong(strings[0]);
				t1 += Long.parseLong(strings[1]);
				t2 += Long.parseLong(strings[2]);
				t3 += Long.parseLong(strings[3]);
			}
			k3.set(k2 + "\t" + t0 + "\t" + t1 + "\t" + t2 + "\t" + t3);
			context.write(k3, NullWritable.get());
		}
	}

	public static class TextArrayWritable extends ArrayWritable {
		public TextArrayWritable() {
			super(Text.class);
		}

		public TextArrayWritable(String[] strings) {
			super(Text.class);
			Text[] texts = new Text[strings.length];
			for (int i = 0; i < strings.length; i++) {
				texts[i] = new Text(strings[i]);
			}
			set(texts);

		}
	}
}*/


//method two
/**
 * 两个方法的区别在于在map端构造v2时是以字符串数组传入还是以Text类型数组传入;如果以字符串数组传入，因为ArrayWritable本身没有
 * 该构造方法，所以必须自定义一个以字符串数组为参数的构造方法来将字符串数组转化为ArrayWritable里定义的成员变量Writable[];
 * 而当以Text类型传入时，是调用ArrayWritable的set方法来实现的，Text类型本身就是序列化类型，所以无需再自行转化
 * @author martin
 *
 */
public class KpiArrayWritableTest {

	public static void main(String[] args) throws Exception {
		final String INPUT_PATH = args[0];
		final String OUTPUT_PATH = args[1];
		Configuration conf = new Configuration();
		FileSystem fileSystem = FileSystem.get(new URI(INPUT_PATH), conf);
		if (fileSystem.exists(new Path(OUTPUT_PATH))) {
			fileSystem.delete(new Path(OUTPUT_PATH), true);
		}

		Job job = Job.getInstance(conf,
				KpiArrayWritableTest.class.getSimpleName());
		job.setJarByClass(KpiArrayWritableTest.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(TextArrayWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		FileInputFormat.setInputPaths(job, new Path(INPUT_PATH));
		FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));
		job.waitForCompletion(true);
	}

	public static class MyMapper extends
			Mapper<LongWritable, Text, Text, TextArrayWritable> {
		Text k2 = new Text();
		TextArrayWritable v2 = new TextArrayWritable();

		@Override
		protected void map(
				LongWritable k1,
				Text v1,
				Mapper<LongWritable, Text, Text, TextArrayWritable>.Context context)
				throws IOException, InterruptedException {
			String[] split = v1.toString().split("\t");
			k2.set(split[1]);
			Text[] text = new Text[]{new Text(split[6]),new Text(split[7]),new Text(split[8]),new Text(split[9])};
			v2.set(text);
			context.write(k2, v2);
		}
	}

	public static class MyReducer extends
			Reducer<Text, TextArrayWritable, Text, NullWritable> {
		Text k3 = new Text();

		@Override
		protected void reduce(
				Text k2,
				Iterable<TextArrayWritable> v2s,
				Reducer<Text, TextArrayWritable, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			long t0 = 0;
			long t1 = 0;
			long t2 = 0;
			long t3 = 0;
			for (TextArrayWritable v2 : v2s) {
				String[] strings = v2.toStrings();
				t0 += Long.parseLong(strings[0]);
				t1 += Long.parseLong(strings[1]);
				t2 += Long.parseLong(strings[2]);
				t3 += Long.parseLong(strings[3]);
			}
			k3.set(k2 + "\t" + t0 + "\t" + t1 + "\t" + t2 + "\t" + t3);
			context.write(k3, NullWritable.get());
		}
	}

	public static class TextArrayWritable extends ArrayWritable {
		public TextArrayWritable() {
			super(Text.class);
		}

	}
}
