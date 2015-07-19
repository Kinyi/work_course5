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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DBInputFormatTest {

//	private static final String OUTPUT_PATH = "hdfs://crxy2:9000/out";
	private static final String OUTPUT_PATH = "hdfs://122.0.67.167:8020/18681163341/out";

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();                               //陈昊的阿里云MySQL账号
		DBConfiguration.configureDB(conf, "com.mysql.jdbc.Driver", "jdbc:mysql://182.92.229.188:3306/test", "root", "19901006x");
//		DBConfiguration.configureDB(conf, "com.mysql.jdbc.Driver", "jdbc:mysql://hadoop0:3306/test", "root", "admin");
		
		FileSystem fileSystem = FileSystem.get(new URI(OUTPUT_PATH), conf);
		if (fileSystem.exists(new Path(OUTPUT_PATH))) {
			fileSystem.delete(new Path(OUTPUT_PATH), true);
		}
		
		Job job = Job.getInstance(conf, DBInputFormatTest.class.getSimpleName());
		job.setJarByClass(DBInputFormatTest.class);
		FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));
		job.setMapperClass(MyMapper.class);
		
		job.setInputFormatClass(DBInputFormat.class);
		DBInputFormat.setInput(job, BlogNewsWritable.class, "select id,name from user", "select count(1) from user");
		
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(BlogNewsWritable.class);
		job.waitForCompletion(true);
	}
	
	public static class MyMapper extends Mapper<LongWritable, BlogNewsWritable, LongWritable, BlogNewsWritable>{
		
		@Override
		protected void map(LongWritable k1, BlogNewsWritable v1,
				Mapper<LongWritable, BlogNewsWritable, LongWritable, BlogNewsWritable>.Context context)
				throws IOException, InterruptedException {
			   context.write(k1, v1);
		}
	}
	
	public static class BlogNewsWritable implements Writable,DBWritable{
		int id;
		String name;

		@Override
		public void write(PreparedStatement statement) throws SQLException {
			statement.setInt(1, id);
			statement.setString(2, name);
		}

		@Override
		public void readFields(ResultSet resultSet) throws SQLException {
			this.id = resultSet.getInt(1);
			this.name = resultSet.getString(2);
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeInt(id);
			out.writeUTF(name);
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			this.id = in.readInt();
			this.name = in.readUTF();
		}

		@Override
		public String toString() {
			return "BlogNewsWritable [id=" + id + ", name=" + name + "]";
		}
	}
}
