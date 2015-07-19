package writable;

import java.io.File;
import java.net.URI;
import java.util.Collection;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

public class SequenceFileTest2 {

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://crxy2:9000/"), conf);
		Path name = new Path("/sf_logs");
		//写操作
		SequenceFile.Writer writer = new SequenceFile.Writer(fs, conf, name, Text.class, BytesWritable.class);
		Collection<File> listFiles = FileUtils.listFiles(new File("/usr/local/hadoop-2.6.0/logs"), new String[]{"log"}, false);
		for (File file : listFiles) {
			String fileName = file.getName();
			Text key = new Text(fileName);
			byte[] readFileToByteArray = FileUtils.readFileToByteArray(file);
			BytesWritable value = new BytesWritable(readFileToByteArray);
			writer.append(key, value);
		}
		IOUtils.closeStream(writer);
		
		//读操作
		SequenceFile.Reader reader = new SequenceFile.Reader(fs, name, conf);
		Text key = new Text();
		BytesWritable val = new BytesWritable();
		while (reader.next(key, val)) {
			File file = new File("/usr/local/hadoop-2.6.0/logs_bak/"+key.toString());
			FileUtils.writeByteArrayToFile(file, val.getBytes());
		}
		IOUtils.closeStream(reader);
	}
}
