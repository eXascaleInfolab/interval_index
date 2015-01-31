package dfs_interval_index;


import dfs_interval_index.TDFSIntervaIndex;
import dfs_interval_index.TMapReduceSearcher;
import dfs_interval_index.TSpatialHadoop;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.*;

import java.io.IOException;
import java.util.*;


public class TTests {
	final static double MAX_START = 10000000000.0;
	final static double MIN_START = 0.0;	
	
	public TTests() {
	}
	
	/**
	 * Performs speed tests for Mapreduce, Spatial hadoop and Distributed interval index
	 */
	public static void main(String[] args) throws IOException {
		
		if (args.length != 2) {
			System.out.println("Parameters: <path to cluster config-files> <DFS folder for files>");
			System.exit(0);
		}
		String configPath = args[0];
		String DFSRoot = args[1];
		
		Configuration config = new Configuration();
		{
			config.addResource(new Path(configPath + "/core-site.xml"));
			config.addResource(new Path(configPath + "/hdfs-site.xml"));
			config.addResource(new Path(configPath + "/mapred-site.xml"));
			config.addResource(new Path(configPath + "/yarn-site.xml"));
		}
		
		FileSystem fs = FileSystem.get(config);
		
		//long[] datasetSizes = {(long)Math.pow(10, 7), (long)Math.pow(10, 8), (long)Math.pow(10, 9)};
		//int[] avgOverlappings = {10, 100, 10000};
		long[] datasetSizes = {(long)Math.pow(10, 6)};
		int[] avgOverlappings = {10};	

		//create datasets
		ArrayList<Path> datasets = new ArrayList<Path>();
		
		for (int sizeIndex = 0; sizeIndex < datasetSizes.length; ++sizeIndex) {
			for (int overlappingIndex = 0; overlappingIndex < avgOverlappings.length; ++overlappingIndex) {
				final long datasetSize = datasetSizes[sizeIndex];
				final int avgOverlapping = avgOverlappings[overlappingIndex];
				System.out.println(datasetSize);
				Path filePath = GenerateDataset(fs, DFSRoot, datasetSize, avgOverlapping);
				datasets.add(filePath);
				System.out.println(".. created dataset: " + filePath.toString());
			}
		}
		
		//create distributed interval index
		for (Path datsetPath : datasets) {
			long time = System.currentTimeMillis();
			new TDFSIntervaIndex(datsetPath.toString(), fs, config, false);
			long delta = System.currentTimeMillis() - time;
			System.out.println(String.format("CONSTRUCTION_TIME DFSIntervalIndex %s %d", datsetPath.toString(), delta));
		}
		
		//create spatial hadoop
		for (Path datsetPath : datasets) {
			long time = System.currentTimeMillis();
			{
				TSpatialHadoop spatialHadoop = new TSpatialHadoop(fs, config);
				spatialHadoop.Convert2MBR(0, MAX_START + 20000, datsetPath.toString(), datsetPath.toString() + ".mbr");
				spatialHadoop.CreateIndex(datsetPath.toString() + ".mbr", datsetPath.toString() + ".shindex", "r+tree");
				fs.delete(new Path(datsetPath.toString() + ".mbr"), true);
			}
			long delta = System.currentTimeMillis() - time;
			System.out.println(String.format("CONSTRUCTION_TIME SpatialHadoop %s %d", datsetPath.toString(), delta));
		}

		
		//query performance
		{
			
			for (Path datasetPath : datasets) {
				System.out.println("RESPONSE_TIMES_FOR " + datasetPath.toString());
				TDFSIntervaIndex index = new TDFSIntervaIndex(datasetPath.toString(), fs, config);
				{//warmup
//					index.ReadsCount = 0;
//					double responseSize = 0;
//					long startTime = System.currentTimeMillis();
//					final int II_QUERIES = 1000000;
//					for (int queryIndex = 0; queryIndex < II_QUERIES; ++queryIndex) {
//						double queryStart = randomizer.nextDouble() * (MAX_START - MIN_START) + MIN_START;
//						responseSize += index.Search(queryStart, queryStart + 1000);
//					}					
				}					
				
				double[] queryLengths = {10000, 100};
				for (int lengthIndex = 0; lengthIndex < queryLengths.length; ++lengthIndex) {
					double queryLength = queryLengths[lengthIndex];
					final int ATTEMPTS = 100;
					for (int attempt = 0; attempt < ATTEMPTS; ++attempt) {
						final int SEED = attempt; 
						{
							Random randomizer = new Random(SEED);
							index.ReadsCount = 0;
							double responseSize = 0;
							long startTime = System.currentTimeMillis();
							final int II_QUERIES = 1;
							for (int queryIndex = 0; queryIndex < II_QUERIES; ++queryIndex) {
								double queryStart = randomizer.nextDouble() * (MAX_START - MIN_START) + MIN_START;
								responseSize += index.Search(queryStart, queryStart + queryLength);
							}
							double timeDelta = (double)(System.currentTimeMillis() - startTime) / II_QUERIES;
							responseSize = responseSize / II_QUERIES;
							double readsCount = (double)index.ReadsCount / II_QUERIES;
							System.out.println(String.format("RESPONSE_TIME DFSIntervalIndex read_count %f qlen %f response_size %f time %f", 
																			readsCount, queryLength, responseSize, timeDelta));						
						}
						{
							
							TSpatialHadoop shIndex = new TSpatialHadoop(fs, config);
							long startTime = System.currentTimeMillis();
							Random randomizer = new Random(SEED);
							double queryStart = randomizer.nextDouble() * (MAX_START - MIN_START) + MIN_START;
							long responseSize = shIndex.Query(queryStart, queryStart + queryLength, datasetPath.toString() + ".shindex", "r+tree");
							long timeDelta = System.currentTimeMillis() - startTime;
							System.out.println(String.format("RESPONSE spatial_hadoop qlen %f response_size %d time %d", 
																					queryLength, responseSize, timeDelta));
						}
						
						{
							TMapReduceSearcher MapReduceSearch = new TMapReduceSearcher(datasetPath.toString(), fs, config);
							long startTime = System.currentTimeMillis();
							Random randomizer = new Random(SEED);
							double queryStart = randomizer.nextDouble() * (MAX_START - MIN_START) + MIN_START;
							TInterval[] queries = {new TInterval(queryStart, queryStart + queryLength, 1)};
							long responseSize = MapReduceSearch.Search(queries);
							long timeDelta = System.currentTimeMillis() - startTime;
							System.out.println(String.format("RESPONSE map_reduce qlen %f response_size %d time %d", 
																					queryLength, responseSize, timeDelta));
						}
					}
				}
			}
		}
	}
	
	public static Path GenerateDataset(FileSystem HDFS, String path, final long size, final int avgOverlapping) throws IOException {
		double intervalCell = (MAX_START - MIN_START) / size;
		double optimalLength = intervalCell * avgOverlapping + 0.001;
		String filename = String.format(path + "/intervals_%d_%d.txt", size, avgOverlapping);
		HDFS.delete(new Path(filename), true);
		FSDataOutputStream file = HDFS.create(new Path(filename));
		Random randomizer = new Random(0);
		for (long index = 0; index < size; ++index) {
			double start = randomizer.nextDouble() * (MAX_START - MIN_START) + MIN_START;
			file.writeBytes(String.format("%f %f %d\n", start, optimalLength, index + 1));
		}
		file.close();
		return new Path(filename);
	}

}
