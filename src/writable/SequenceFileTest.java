package writable;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

public class SequenceFileTest {

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://122.0.67.167:8020/"), conf);
		Path name = new Path("/18681163341/sf");
		//写操作
		SequenceFile.Writer writer = new SequenceFile.Writer(fs, conf, name, LongWritable.class, Text.class);
		for (int i = 0; i < 4; i++) {
			writer.append(new LongWritable(i), new Text(i+"xxx"));
		}
		IOUtils.closeStream(writer);
		//读操作
		SequenceFile.Reader reader = new SequenceFile.Reader(fs, name, conf);
		LongWritable key = new LongWritable();
		Text val = new Text();
		while (reader.next(key, val)) {
			System.out.println(key.get()+"\t"+val.toString());
		}
		IOUtils.closeStream(reader);
	}
}
