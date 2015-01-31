package dfs_interval_index;

import dfs_interval_index.TInterval;
import dfs_interval_index.TSortMR;

import java.nio.ByteBuffer;
import java.io.IOException;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.PriorityQueue;

import org.apache.hadoop.hdfs.client.HdfsDataInputStream;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class TDFSIntervaIndex {
	
	private int CheckpointInterval = 201; //dummy value
	public static final int PAGE_SIZE = 65536;
	public static final int MIN_BLOCK_SIZE = 512;
	private ArrayList<TSkipListElem> SkipList;
	private String IndexFilePath;
	
	public long CheckpointSearches;
	public long ReadsCount;
	
	FSDataInputStream IndexFile;
	FSDataInputStream CheckpointsFile;
	
	
	public TDFSIntervaIndex() {
		// TODO Auto-generated constructor stub
	}
	
	
	private class TRightBorderSorter implements Comparable<TRightBorderSorter> {
		public TInterval Interval;
		public TRightBorderSorter() {
		}
		public TRightBorderSorter(TInterval interval) {
			Interval = new TInterval(interval);
		}
		public boolean LessThan(TRightBorderSorter second) {
			if (Interval.End != second.Interval.End) {
				return Interval.End < second.Interval.End;
			} else {
				return Interval.LessThan(second.Interval);
			}
		}
		public boolean Equal(TRightBorderSorter second) {
			return Interval.Equal(second.Interval);
		}
		public int CompareValue(TRightBorderSorter second) {
			if (Equal(second)) {
				return 0;
			}
			if (LessThan(second)) {
				return -1;
			}
			return 1;
		}

		@Override
		public int compareTo(TRightBorderSorter second) {
			return CompareValue(second);
		}
	}
	
	private class TRightBorderReversedSorter  implements Comparable<TRightBorderReversedSorter> {
		public TInterval Interval;

		public TRightBorderReversedSorter(TInterval interval) {
			Interval = new TInterval(interval);
		}		
		
		public boolean LessThan(TRightBorderReversedSorter second) {
			if (Interval.End != second.Interval.End) {
				return Interval.End > second.Interval.End;
			} else {
				return !Interval.LessThan(second.Interval);
			}
		}
		public boolean Equal(TRightBorderReversedSorter second) {
			return Interval.Equal(second.Interval);
		}
		public int CompareValue(TRightBorderReversedSorter second) {
			if (Equal(second)) {
				return 0;
			}
			if (LessThan(second)) {
				return -1;
			}
			return 1;
		}
		@Override
		public int compareTo(TRightBorderReversedSorter second) {
			return CompareValue(second);
		}
	}
	
	private class TSkipListElem {
		long Offset;
		double MinLeftBorder;
		TSkipListElem(long offset, double minLeftBorder) {
			Offset = offset;
			MinLeftBorder = minLeftBorder;
		}
		
	}
	
	
	public TDFSIntervaIndex(String indexFilePath, FileSystem hdfs, Configuration config) throws IOException {
		IndexFilePath = indexFilePath;
		IndexFile = hdfs.open(new Path(IndexFilePath + ".index"));
		CheckpointsFile = hdfs.open(new Path(IndexFilePath + ".checkpoints"));	
		//cache block locations
		((HdfsDataInputStream)IndexFile).getAllBlocks();
		((HdfsDataInputStream)IndexFile).getAllBlocks();
		
		FSDataInputStream metaFile = hdfs.open(new Path(IndexFilePath + ".meta"));
		CheckpointInterval = (int)metaFile.readLong();
		long skiplistSize = metaFile.readLong();
		SkipList = new ArrayList<TDFSIntervaIndex.TSkipListElem>();
		for (int elemIndex = 0; elemIndex < skiplistSize; ++elemIndex) {
			SkipList.add(new TSkipListElem(metaFile.readLong(), metaFile.readDouble()));

		}
		metaFile.close();

		
	}
	
	
	public TDFSIntervaIndex(String sourceFilePath,
			                FileSystem hdfs, 
			                Configuration config, 
			                boolean verbose) throws IOException {	
		
		
		
		boolean preSort = true;
		boolean calcOptimalCheckpointInterval = true;
		boolean buildIndex = true;
		String sortedFile = sourceFilePath + ".sorted";
		
		if (preSort) {//sort intervals	
			hdfs.delete(new Path(sortedFile), true);	
			JobConf jobConfig = new JobConf(config);
			jobConfig.setJobName("sort_intervals");
			
			jobConfig.setMapOutputKeyClass(dfs_interval_index.TInterval.class);
			jobConfig.setMapOutputValueClass(NullWritable.class);			
			
		    jobConfig.setOutputKeyClass(dfs_interval_index.TInterval.class);
			jobConfig.setOutputValueClass(NullWritable.class);
			
			jobConfig.setMapperClass(dfs_interval_index.TSortMR.Map.class);
			jobConfig.setReducerClass(dfs_interval_index.TSortMR.Reduce.class);
			jobConfig.setInputFormat(TextInputFormat.class);
			jobConfig.setOutputFormat(SequenceFileOutputFormat.class);		
			//NB: everything will be joined into one file
			jobConfig.setNumReduceTasks(1);
			
			String jarPath = dfs_interval_index.TSortMR.Map.class.getProtectionDomain().getCodeSource().getLocation().getPath();
			if (jarPath.endsWith("jar")) {
				jobConfig.setJar(jarPath);
				if (verbose) System.out.println("JAR: " + jarPath);
			} else {
				//TODO: replace
				jobConfig.setJar("/home/arslan/src/1d_interval_index/src/hadoop/dfs_interval_index.jar");
			}
			FileInputFormat.setInputPaths(jobConfig, new Path(sourceFilePath));
			FileOutputFormat.setOutputPath(jobConfig, new Path(sortedFile));
			try {
				JobClient.runJob(jobConfig);
			} catch ( java.lang.IllegalArgumentException exception) {
			}
		}
		
		//since 1 reducer on the previous stage, the sorted file is merged	
		Path sortedFilePath = new Path(sortedFile + "/part-00000");
		
		if (calcOptimalCheckpointInterval) {
		    //Path overlappingsFilePath = new Path(sourceFilePath + ".overallpings");
		    //hdfs.delete(overlappingsFilePath, true);	
		    //FSDataOutputStream overlappingsFile = hdfs.create(overlappingsFilePath);		    
		    ArrayList<Integer> overlappingsSample = new ArrayList<Integer>();	
		    //TODO: replace deprecated method
		    SequenceFile.Reader reader = new SequenceFile.Reader(hdfs, sortedFilePath, config);
			PriorityQueue<Double> rightBordersHeap = new PriorityQueue<Double>();
			TInterval key = new TInterval();
			NullWritable nullWritable = NullWritable.get();	
			int processed = 0;
			final int SAMPLE_SIZE = 1000000;
			double max_len = 0;
			while (reader.next(key, nullWritable)) {
				max_len = Math.max(key.End - key.Start, max_len);
				while (rightBordersHeap.peek() != null) {
					if (rightBordersHeap.peek() < key.Start) {
						rightBordersHeap.poll();
					} else {
						break;
					}
				}
				overlappingsSample.add(rightBordersHeap.size());
				rightBordersHeap.add(key.End);
				{
					++processed;
					
					if (processed % 500000 == 0 && verbose) {
						System.out.println(String.format("..processed, %d", processed));
					}
					if (processed >= SAMPLE_SIZE) {
						break;
					}
				}
			}
			//overlappingsFile.close();
			{
				double avgOverlapping = 0.0;
				for (int pos = 0; pos < overlappingsSample.size(); ++pos) {
					avgOverlapping += overlappingsSample.get(pos);
				}
				avgOverlapping = avgOverlapping / overlappingsSample.size();
				if (verbose) System.out.println(String.format("Avg overlapping: %f", avgOverlapping));
				
				
				double maxMemoryUsage = overlappingsSample.size();//spaceFactor == 1
				for (CheckpointInterval = 1; CheckpointInterval <= overlappingsSample.size(); ++CheckpointInterval) {
					int offset = 0;
					Long memoryUsage = new Long(0);
					while (offset < overlappingsSample.size()) {
						memoryUsage += overlappingsSample.get(offset);
						offset += CheckpointInterval;
					}
					if (memoryUsage <= maxMemoryUsage) {
						break;
					}
				}
			}
		}
		
		IndexFilePath = sourceFilePath;
		HdfsDataOutputStream indexFile = (HdfsDataOutputStream)hdfs.create(new Path(IndexFilePath + ".index"), true);
		HdfsDataOutputStream checkpointsFile = (HdfsDataOutputStream)hdfs.create(new Path(IndexFilePath + ".checkpoints"), true);
		long indexFileSize = indexFile.size();
		long checkpointsFileSize = checkpointsFile.size();
		
		
		FSDataOutputStream metaFile = hdfs.create(new Path(IndexFilePath + ".meta"));		
		metaFile.writeLong(CheckpointInterval);
		if (verbose) System.out.println(String.format("Optimal checkpoint interval: %d", CheckpointInterval));
		if (buildIndex) {
			SkipList = new ArrayList<TSkipListElem>();
			PriorityQueue<TRightBorderSorter> rightBordersHeap = new PriorityQueue<TRightBorderSorter>();
			SequenceFile.Reader reader = new SequenceFile.Reader(hdfs, sortedFilePath, config);
			long pageIndex = 0;
			int recordIndex = 0;
			TInterval previousInterval = null;
			TInterval key = new TInterval();
			NullWritable nullWritable = NullWritable.get();	
			while (reader.next(key, nullWritable)) {	
				if (recordIndex % 50000 == 0 && verbose) {
					System.out.println(String.format("..processed %d", recordIndex));
				}
				if (recordIndex % CheckpointInterval == 0) {
					{//leave only intervals which continue
						while (rightBordersHeap.peek() != null) {
							if (rightBordersHeap.peek().Interval.End < key.Start) {
								rightBordersHeap.poll();
							} else {
								break;
							}
						}
					}
					try {
						//write a checkpoint record
						final long checkpointsSizeBeforeInsert = checkpointsFileSize;
						double farestRigtBorder = -1;
						{//write to checkpoints file
							checkpointsFile.writeInt(rightBordersHeap.size());
							checkpointsFileSize += 4;
							PriorityQueue<TRightBorderReversedSorter> rightBordersReversedHeap = 
										new PriorityQueue<TRightBorderReversedSorter>();
							Iterator<TRightBorderSorter> iterator = rightBordersHeap.iterator();
							while (iterator.hasNext()) {
								TRightBorderReversedSorter element = new TRightBorderReversedSorter(iterator.next().Interval);
								rightBordersReversedHeap.add(element);
							}
							if (rightBordersReversedHeap.peek() != null) {
								farestRigtBorder = rightBordersReversedHeap.peek().Interval.End;
							}							
							while (rightBordersReversedHeap.peek() != null) {
								TInterval interval = rightBordersReversedHeap.poll().Interval;
								checkpointsFile.writeDouble(interval.Start);
								checkpointsFile.writeDouble(interval.End);
								checkpointsFile.writeLong(interval.Id);	
								checkpointsFileSize += 8 * 3;
							}
							//while (checkpointsFileSize % MIN_BLOCK_SIZE != 0) {							
							//	checkpointsFile.writeInt(0);
							//	checkpointsFileSize += 4;
							//}				
						}
						
						{//write to index file
							indexFile.writeLong(checkpointsSizeBeforeInsert);
							indexFile.writeDouble(farestRigtBorder);
							indexFile.writeLong(checkpointsSizeBeforeInsert);
							indexFile.writeLong(checkpointsSizeBeforeInsert);
							indexFileSize += 8 * 4;
							++recordIndex;	
						}
						
					} catch (IOException exception) {
						System.err.println(exception);
					}
				}
				rightBordersHeap.add(new TRightBorderSorter(key));
				long prevPositionInIndex = indexFileSize;
				indexFile.writeDouble(key.Start);
				indexFile.writeDouble(key.End);
				indexFile.writeLong(key.Id);
				indexFile.writeLong(key.Id);
				indexFileSize += 8 * 4;
				++recordIndex;
				long newPageIndex = prevPositionInIndex / PAGE_SIZE;
				if (SkipList.size() == 0 || (newPageIndex > pageIndex) && (previousInterval != null) &&
						key.Start > previousInterval.Start) {
					SkipList.add(new TSkipListElem(prevPositionInIndex, key.Start));
					pageIndex = newPageIndex;
				}
				previousInterval = new TInterval(key);
			}
			SkipList.add(new TSkipListElem(indexFileSize, previousInterval.Start + 1));
			
			{
				metaFile.writeLong(SkipList.size());
				for (int index = 0; index < SkipList.size(); ++index) {
					metaFile.writeLong(SkipList.get(index).Offset);
					metaFile.writeDouble(SkipList.get(index).MinLeftBorder);
				}
			}
			indexFile.close();
			checkpointsFile.close();
			metaFile.close();
			IndexFile = hdfs.open(new Path(IndexFilePath + ".index"));
			CheckpointsFile = hdfs.open(new Path(IndexFilePath + ".checkpoints"));			
		}
	}

	private void SearchInCheckpoint(ArrayList<TInterval> results, 
									final long checkpointLocation, 
									final double start, 
									final double end) throws IOException {
		CheckpointsFile.seek(checkpointLocation);
		int recordsCount = CheckpointsFile.readInt();
		while (recordsCount > 0) {
			double intervalStart = CheckpointsFile.readDouble();
			double intervalEnd = CheckpointsFile.readDouble();
			long intervalId = CheckpointsFile.readLong();
			--recordsCount;
			if (intervalEnd < start) {
				break;
			}
			TInterval result = new TInterval(intervalStart, intervalEnd, intervalId);
			results.add(result);
		}
	}
	
	public long Search(final double start, final double end) throws IOException {
		return Search(start, end, PAGE_SIZE);
	}
	
	public long Search(final double start, final double end, final int READ_SIZE) throws IOException {
		if (end <= start) {
			return 0;
		}		
		if (SkipList.size() == 0) {
			return 0;
		}
		if (SkipList.get(0).MinLeftBorder > end) {
			return 0;
		}
		if (SkipList.get(SkipList.size() - 1).MinLeftBorder <= start) {
			return 0;
		}
		
		long uploadRightBorder = 0;
		int lastRecordIndex = SkipList.size() - 1;
		{
			if (SkipList.get(lastRecordIndex).MinLeftBorder <= end) {
				uploadRightBorder = SkipList.get(lastRecordIndex).Offset;
			}
			if (SkipList.get(0).MinLeftBorder > end) {
				uploadRightBorder = SkipList.get(0).Offset;
			}
			int leftIndex = 0;
			int rightIndex = lastRecordIndex;
			while (rightIndex > leftIndex + 1) {
				int middle = (rightIndex + leftIndex) / 2;
				if (SkipList.get(middle).MinLeftBorder > end) {
					rightIndex = middle;
 				} else {
 					leftIndex = middle;
 				}
			}
			uploadRightBorder = SkipList.get(rightIndex).Offset;
		}
		
		ArrayList<TInterval> results = new ArrayList<TInterval>();
		long readTime = 0;
		int checkpointReadCount = 0;
	
		byte[] buffer = new byte[READ_SIZE];
		double nextIntervalStart = end;
		while (uploadRightBorder > 0) {
			final int RECORD_SIZE = 32;
			final int FIELD_SIZE = 8;
			long uploadFrom = (long)(Math.max(0, Math.ceil((double)(uploadRightBorder - READ_SIZE) / RECORD_SIZE))) * RECORD_SIZE;
			
			int length = (int)(uploadRightBorder - uploadFrom);
			
			long readTimeStart = System.currentTimeMillis();
			IndexFile.readFully(uploadFrom, buffer, 0, READ_SIZE);
			++ReadsCount;
			readTime += System.currentTimeMillis() - readTimeStart;
			
			ByteBuffer bufferAsStream = ByteBuffer.wrap(buffer);
			int localProfit = 0;
			boolean lastChunk = false;
			for (int recordIndex = (length / RECORD_SIZE) - 1; recordIndex > -1; --recordIndex) {
				long globalRecordIndex = (uploadFrom / RECORD_SIZE) + recordIndex;
				boolean isLinkToCheckPoints = globalRecordIndex % CheckpointInterval == 0;
				if (isLinkToCheckPoints) {
					//still inside the query, just skip this checkpoint
					if (nextIntervalStart >= start) {
						continue; 
					}
					boolean isFirstLinkInBuffer = recordIndex < CheckpointInterval;
					bufferAsStream.position(recordIndex * RECORD_SIZE);
					long checkpointPosition = bufferAsStream.getLong();
					double farestRightBorderInChArray = bufferAsStream.getDouble();
					
					//no more intervals that overlap the query
					if (farestRightBorderInChArray == -1 || farestRightBorderInChArray < start) {
						lastChunk = true;
						break; //full stop
					}
					//ok, worst case, let's read from checkpoint array
					if (isFirstLinkInBuffer) {
						SearchInCheckpoint(results, checkpointPosition, start, end);
						++CheckpointSearches;
						++checkpointReadCount;
						lastChunk = true;
						break; // full stop 
					}
					//use what we already read, go further
					continue;
				}		
				
				{
					bufferAsStream.position(recordIndex * RECORD_SIZE);
					double intervalStart = bufferAsStream.getDouble();
					double intervalEnd = bufferAsStream.getDouble();
					long intervalId = bufferAsStream.getLong();
					if (intervalEnd >= start && intervalStart <= end) {
						//add to results
						TInterval result = new TInterval();
						result.Id = intervalId;
						result.Start = intervalStart;
						result.End = intervalEnd;
						results.add(result);
						localProfit += 1;
					}
					nextIntervalStart = intervalStart;
				}
			}
			if (lastChunk) {
				break;
			}
			uploadRightBorder = uploadFrom;
		}
		return results.size();
	}	
	
	

	
}
