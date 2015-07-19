package inputformat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URI;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.GenericWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *  MySQL中有个用户表，表结构如下
        id       name
        1        zhangsan
        2        lisi
        3        wangwu
        
    HDFS中有个交易日志表，日志结构如下
        1        12
        2        24
        2        12
        3        45
        
  -------------------------------------
             要求：产生结果形式如下
        lisi     36
        wangwu   45
        zhagnsan 12
        
 * @author martin
 *
 */
/**
 * 错误分析：java.lang.ClassCastException: org.apache.hadoop.mapreduce.lib.db.DBInputFormat$DBInputSplit 
 * cannot be cast to org.apache.hadoop.mapreduce.lib.input.TaggedInputSplit
 * 原因：DBInputFormat.setInput()该语句要写在两条MultipleInputs.addInputPath()的前面
 * @author martin
 *
 */
public class MultipleInputTest {

	private static final String INPUT_PATH = "hdfs://122.0.67.167:8020/18681163341/trade";
	private static final String OUTPUT_PATH = "hdfs://122.0.67.167:8020/18681163341/out";

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		DBConfiguration.configureDB(conf, "com.mysql.jdbc.Driver", "jdbc:mysql://182.92.229.188:3306/test", "root", "19901006x");
		FileSystem fileSystem = FileSystem.get(new URI(INPUT_PATH), conf);
		if (fileSystem.exists(new Path(OUTPUT_PATH))) {
			fileSystem.delete(new Path(OUTPUT_PATH), true);
		}
		Job job = Job.getInstance(conf, MultipleInputTest.class.getSimpleName());
		job.setJarByClass(MultipleInputTest.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(MyGenericWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		job.setReducerClass(MyReducer.class);
		job.setNumReduceTasks(1);
		DBInputFormat.setInput(job, MyDBWritable.class, "select id,name from user", "select count(1) from user");
		MultipleInputs.addInputPath(job, new Path("hdfs://122.0.67.167:8020/18681163341/"), DBInputFormat.class, DBMapper.class);
		MultipleInputs.addInputPath(job, new Path(INPUT_PATH), TextInputFormat.class, HDFSMapper.class);
		FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));
		job.waitForCompletion(true);
	}
	
	public static class DBMapper extends Mapper<LongWritable, MyDBWritable, LongWritable, MyGenericWritable>{
		LongWritable k2 = new LongWritable();
		@Override
		protected void map(LongWritable key,MyDBWritable value,
				Mapper<LongWritable, MyDBWritable, LongWritable, MyGenericWritable>.Context context)
				throws IOException, InterruptedException {
			String[] split = value.toString().split("\t");
			k2.set(Long.parseLong(split[0]));
			MyGenericWritable v2 = new MyGenericWritable(new Text(split[1]));
			context.write(k2, v2);
		}
	}
	
	public static class HDFSMapper extends Mapper<LongWritable, Text, LongWritable, MyGenericWritable>{
		LongWritable k2 = new LongWritable();
		@Override
		protected void map(LongWritable key,Text value,
				Mapper<LongWritable, Text, LongWritable, MyGenericWritable>.Context context)
				throws IOException, InterruptedException {
			String[] split = value.toString().split("\t");
			k2.set(Long.parseLong(split[0]));
			MyGenericWritable v2 = new MyGenericWritable(new LongWritable(Long.parseLong(split[1])));
			context.write(k2, v2);
		}
	}
	
	public static class MyReducer extends Reducer<LongWritable, MyGenericWritable, Text, LongWritable>{
		Text k3 = new Text();
		LongWritable v3 = new LongWritable();
		@Override
		protected void reduce(LongWritable k2,Iterable<MyGenericWritable> v2s,
				Reducer<LongWritable, MyGenericWritable, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			long sum = 0;
			for (MyGenericWritable myGenericWritable : v2s) {
				Writable writable = myGenericWritable.get();
				if (writable instanceof Text) {
					k3.set(writable.toString());
				}else {
					sum += ((LongWritable)writable).get();
				}
			}
			v3.set(sum);
			context.write(k3, v3);
		}
	}
	
	public static class MyDBWritable implements Writable,DBWritable{
		long id;
		String name;
		@Override
		public void write(PreparedStatement statement) throws SQLException {
			statement.setLong(1, id);
			statement.setString(2, name);
		}

		@Override
		public void readFields(ResultSet resultSet) throws SQLException {
			this.id = resultSet.getInt(1);
			this.name = resultSet.getString(2);
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeLong(id);
			out.writeUTF(name);
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			this.id = in.readLong();
			this.name = in.readUTF();
		}

		@Override
		public String toString() {
			return id + "\t" + name;
		}

	}
	
	public static class MyGenericWritable extends GenericWritable{

		public MyGenericWritable() {}
		
		public MyGenericWritable(Text text) {
			super.set(text);
		}
		
		public MyGenericWritable(LongWritable longWritable) {
			super.set(longWritable);
		}

		@SuppressWarnings("unchecked")
		@Override
		protected Class<? extends Writable>[] getTypes() {
			return new Class[]{Text.class,LongWritable.class};
		}
		
	}

}
