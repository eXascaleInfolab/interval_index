package dfs_interval_index;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapreduce.TaskCounter;

import edu.umn.cs.spatialHadoop.core.CellInfo;

public class TMapReduceSearcher {
	
	private Path SourceFilePath;
	private FileSystem HDFS;
	private Configuration Config;

	public TMapReduceSearcher(String sourceFilePath, FileSystem hdfs, Configuration config) {
		SourceFilePath = new Path(sourceFilePath);
		HDFS = hdfs;
		Config = config;
	}
	
	public long Search(TInterval[] queries) throws IOException {
		JobConf jobConfig = new JobConf(Config);
		
		//TODO: replace to .sorted made of TIntervals
		FileInputFormat.setInputPaths(jobConfig, SourceFilePath);
		Path resultsFile = new Path(SourceFilePath + ".qres"); 
		HDFS.delete(resultsFile, true);
		FileOutputFormat.setOutputPath(jobConfig, resultsFile);		
		
		jobConfig.setJobName("search_request_mapreduce");

		jobConfig.setMapOutputKeyClass(dfs_interval_index.TInterval.class);
		jobConfig.setMapOutputValueClass(NullWritable.class);			
		jobConfig.setOutputKeyClass(dfs_interval_index.TInterval.class);
		jobConfig.setOutputValueClass(NullWritable.class);

		jobConfig.setMapperClass(dfs_interval_index.TMapReduceSearcher.TSearchMap.class);
		//Turn-off reduce stage
		jobConfig.setNumReduceTasks(0);
		
		jobConfig.setInputFormat(TextInputFormat.class);
		jobConfig.setOutputFormat(SequenceFileOutputFormat.class);
		jobConfig.setQuietMode(true);
		
		String jarPath = dfs_interval_index.TSortMR.Map.class.getProtectionDomain().getCodeSource().getLocation().getPath();
		if (jarPath.endsWith("jar")) {
			jobConfig.setJar(jarPath);
			System.out.println("JAR: " + jarPath);
		} else {
			//TODO: replace
			jobConfig.setJar("/home/arslan/src/1d_interval_index/src/hadoop/dfs_interval_index.jar");
		}
		
		{//send queries to cache
		    Path tempFile;
		    FileSystem fs = FileSystem.get(jobConfig);
		    do {
		      tempFile = new Path(jobConfig.getJobName()+"_"+(int)(Math.random()*1000000)+".queries");
		    } while (fs.exists(tempFile));
		    FSDataOutputStream out = fs.create(tempFile);
		    out.writeInt(queries.length);
		    for (TInterval query : queries) {
		    	out.writeDouble(query.Start);
		    	out.writeDouble(query.End);
		    }
		    out.close();
		    fs.deleteOnExit(tempFile);
		    DistributedCache.addCacheFile(tempFile.toUri(), jobConfig);
		    jobConfig.set("queries_file", tempFile.getName());
		}		
		
		
		RunningJob runningJob = JobClient.runJob(jobConfig);
		Counters counters = runningJob.getCounters();
		Counter outputRecordCounter = counters.findCounter(TaskCounter.MAP_OUTPUT_RECORDS);
		final long resultCount = outputRecordCounter.getValue();
		
		/*
		long numberOfResults = 0;
		FileStatus[] reduceFiles = HDFS.listStatus(new Path(resultsFile + "/"));
		for (FileStatus status : reduceFiles) {
	        Path path = status.getPath();
	        if (path.toString().endsWith("_SUCCESS")) {
	        	continue;
	        }
			SequenceFile.Reader reader = new SequenceFile.Reader(HDFS, path, Config);
			TInterval key = new TInterval();
			NullWritable nullWritable = NullWritable.get();	
			while (reader.next(key, nullWritable)) {
				++numberOfResults;
			}
	    }
	    */
		return resultCount;
		
	}
	
	public static class TSearchMap extends MapReduceBase implements Mapper<LongWritable, Text, TInterval, NullWritable> {
		private TInterval[] Queries;
		public void configure(JobConf job) {
			Queries = null;
			try {
				String queriesFile = job.get("queries_file");
				if (queriesFile != null) {
					Path[] cacheFiles = DistributedCache.getLocalCacheFiles(job);
					for (Path cacheFile : cacheFiles) {
						if (cacheFile.getName().contains(queriesFile)) {
							FSDataInputStream in = FileSystem.getLocal(job).open(cacheFile);
							int queriesCount = in.readInt();
							Queries = new TInterval[queriesCount];
							for (int queryIndex = 0; queryIndex < queriesCount; ++queryIndex) {
								Queries[queryIndex] = new TInterval();
								Queries[queryIndex].Start = in.readDouble();
								Queries[queryIndex].End = in.readDouble();
							}
							in.close();
						}
					}
				}
			} catch (IOException e) {
			}
		}
		
		public void map(LongWritable key, Text value,
						OutputCollector<TInterval, NullWritable> output, 
						Reporter reporter) throws IOException {
			String[] chunks = value.toString().split(" ");
			Double start = Double.parseDouble(chunks[0]);
			Double end = start + Double.parseDouble(chunks[1]);
			Integer id = Integer.parseInt(chunks[2]);
			// report FOR EACH query which it overlaps
			for (int queryIndex = 0; queryIndex < Queries.length; ++queryIndex) {
				if (start <= Queries[queryIndex].End && end >= Queries[queryIndex].Start) {
					TInterval interval = new TInterval(start, end, id);		
					output.collect(interval, NullWritable.get());
				}
			}
		}
	}

}
