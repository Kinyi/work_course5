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

/**
 * 使用SequenceFile把音视频文件写入到HDFS，然后读取回来，确认还能够正常播放音视频文件
 * @author martin
 *
 */
public class SequenceFileHomework {

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {
		//写数据
		Configuration conf = new Configuration();
		FileSystem fileSystem = FileSystem.get(new URI("hdfs://122.0.67.167:8020/"), conf);
		Path name = new Path("/18681163341/sf_mp3");
		SequenceFile.Writer writer = new SequenceFile.Writer(fileSystem, conf, name, Text.class, BytesWritable.class);
		Collection<File> listFiles = FileUtils.listFiles(new File("/home/18681163341/"), new String[]{"mp3"}, false);
		for (File file : listFiles) {
			String fileName = file.getName();
			Text key = new Text(fileName);
			byte[] readFileToByteArray = FileUtils.readFileToByteArray(file);
			BytesWritable value = new BytesWritable(readFileToByteArray);
			writer.append(key, value);
		}
		IOUtils.closeStream(writer);
		
		//读数据
		SequenceFile.Reader reader = new SequenceFile.Reader(fileSystem, name, conf);
		Text out_key = new Text();
		BytesWritable out_value = new BytesWritable();
		while (reader.next(out_key, out_value) ) {
			File file = new File("/home/18681163341/sf.mp3");
			FileUtils.writeByteArrayToFile(file, out_value.getBytes());
		}
		IOUtils.closeStream(reader);
	}

}
