package dfs_interval_index;


import java.io.IOException;
import java.util.*;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.JobConf;



import dfs_interval_index.TInterval;

public final class TSearchMR {
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, TInterval, NullWritable> {
		private static Double QueryStart;
		private static Double QueryEnd;
		public void configure(JobConf job) {
			QueryStart = Double.parseDouble(job.get("query_interval_start"));
			QueryEnd = Double.parseDouble(job.get("query_interval_end"));
		}
		public void map(LongWritable key, Text value,
						OutputCollector<TInterval, NullWritable> output, 
						Reporter reporter) throws IOException {
			String[] chunks = value.toString().split(" ");
			Double start = Double.parseDouble(chunks[0]);
			Double end = start + Double.parseDouble(chunks[1]);
			Integer id = Integer.parseInt(chunks[2]);		
			if (start <= QueryEnd && end >= QueryStart) {
				TInterval interval = new TInterval(start, end, id);		
				output.collect(interval, NullWritable.get());
			}
		}
	}
	public static class Reduce extends MapReduceBase implements Reducer<TInterval, NullWritable, TInterval, NullWritable> {
		public void reduce(TInterval key, Iterator<NullWritable> values, 
						   OutputCollector<TInterval, NullWritable> output, 
						   Reporter reporter) throws IOException {
			output.collect(key, NullWritable.get());
		}
	}
}