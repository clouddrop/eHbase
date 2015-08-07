package samar.hbase.mr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

public class HbaseMRSimpleRead {

	public static void main(String args[]) throws IOException, InterruptedException, ClassNotFoundException {
		String tableName = "t1";

		Configuration config = HBaseConfiguration.create();
		Job job = new Job(config, "ExampleRead");
		job.setJarByClass(HbaseMRSimpleRead.class); // class that contains
													// mapper

		Scan scan = new Scan();
		scan.setCaching(500); // 1 is the default in Scan, which will be bad for
								// MapReduce jobs
		scan.setCacheBlocks(false); // don't set to true for MR jobs
		// set other scan attrs

		TableMapReduceUtil.initTableMapperJob(tableName, // input HBase table
															// name
				scan, // Scan instance to control CF and attribute selection
				MyMapper.class, // mapper
				null, // mapper output key
				null, // mapper output value
				job);
		job.setOutputFormatClass(NullOutputFormat.class); // because we aren't
															// emitting anything
															// from mapper

		boolean b = job.waitForCompletion(true);
		if (!b) {
			throw new IOException("error with job!");
		}
	}

}

class MyMapper extends TableMapper<Text, Text> {

	public void map(ImmutableBytesWritable row, Result value, Context context) throws InterruptedException, IOException {
		System.out.println("row  :" + new String(row.get()) + " value: " + value.getColumn("f1".getBytes(), "q1".getBytes()));
		// String value1= new String(value.getColumn("f1".getBytes(),
		// "q1".getBytes()));

	}
}

class MyRedducer extends TableReducer<Text, Text, Text> {
	protected void reduce(Text arg0, java.lang.Iterable<Text> arg1, org.apache.hadoop.mapreduce.Reducer<Text, Text, Text, org.apache.hadoop.io.Writable>.Context arg2) throws IOException, InterruptedException {
	};

}

class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	private static int counter = 0;
	private IntWritable result = new IntWritable();

	public IntSumReducer() {
		counter++;
	}

	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

		System.out.println("I am here Reducer " + counter);
		int sum = 0;
		for (IntWritable val : values) {
			sum += val.get();
		}
		result.set(sum);
		context.write(key, result);
	}
}
