package kpi;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.VLongWritable;

public class VLongWritableTest {

	public static void main(String[] args) throws Exception {
		long long1 = 999999999999999999L;
		LongWritable l1 = new LongWritable(long1);
		VLongWritable l2 = new VLongWritable(long1);
		
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		DataOutputStream dos = new DataOutputStream(baos);
		l2.write(dos);
		System.out.println(dos.size());
	}

}
