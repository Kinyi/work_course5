package group;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URI;

/**
 * Created by RandyChan on 15/2/2.
 */
public class SortGroupApp {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI(args[0]), conf);
		if (fileSystem.exists(new Path(args[1]))) {
			fileSystem.delete(new Path(args[1]), true);
		}
		
        Job job = Job.getInstance(conf, SortGroupApp.class.getSimpleName());
        job.setJarByClass(SortGroupApp.class);

        //输入输出路径
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //输入输出格式化类

        job.setGroupingComparatorClass(NewK2.class);

        //map、reduce
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReduce.class);

        //map输出类型
        job.setMapOutputKeyClass(NewK2.class);
        job.setMapOutputValueClass(IntWritable.class);

        //reduce输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        //提交
        job.waitForCompletion(true);
    }

    public static class MyMapper extends Mapper<LongWritable, Text, NewK2, IntWritable> {
        private static final IntWritable V2 = new IntWritable();
        private static final NewK2 K2 = new NewK2();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] splited = value.toString().split("\t");
            int first = Integer.parseInt(splited[0]);
            int second = Integer.parseInt(splited[1]);
            K2.set(first, second);
            V2.set(first);
//            V2.set(second);
            context.write(K2, V2);
        }
    }

    public static class MyReduce extends Reducer<NewK2, IntWritable, Text, NullWritable> {
        private static final Text K3 = new Text();

        @Override
        protected void reduce(NewK2 key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int max = 0;
            for (IntWritable value : values) {
                if (max < value.get()) {
                    max = value.get();
                }
            }

//            K3.set(max + "\t" + key.getFirst());
            K3.set(max + "\t" + key.getSecond());
            context.write(K3, NullWritable.get());
        }
    }

    public static class NewK2 implements WritableComparable<NewK2>, RawComparator<NewK2> {
        private int first;
        private int second;

        public NewK2() {
        }

        public NewK2(int first, int second) {
            set(first, second);
        }

        private void set(int first, int second) {
            this.first = first;
            this.second = second;
        }

        @Override
        public int compareTo(NewK2 o) {
            int minus = this.first - o.first;
            if (minus == 0) {
                minus = this.second - o.second;
            }
            return minus;
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return WritableComparator.compareBytes(b1, s1, 4, b2, s2, 4);
        }

        @Override
        public int compare(NewK2 o1, NewK2 o2) {
            return 0;
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeInt(this.first);
            dataOutput.writeInt(this.second);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            set(dataInput.readInt(), dataInput.readInt());
        }

        public int getFirst() {
            return first;
        }

        public int getSecond() {
            return second;
        }
    }
}

