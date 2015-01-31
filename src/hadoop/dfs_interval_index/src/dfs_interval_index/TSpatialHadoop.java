package dfs_interval_index;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hdfs.client.HdfsDataInputStream;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.util.GenericOptionsParser;

import com.vividsolutions.jts.index.SpatialIndex;

import edu.umn.cs.spatialHadoop.core.CellInfo;
import edu.umn.cs.spatialHadoop.core.GlobalIndex;
import edu.umn.cs.spatialHadoop.core.GridInfo;
import edu.umn.cs.spatialHadoop.core.Partition;
import edu.umn.cs.spatialHadoop.core.Point;
import edu.umn.cs.spatialHadoop.core.RTree;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.ResultCollector;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.core.SpatialSite;
import edu.umn.cs.spatialHadoop.io.TextSerializerHelper;
import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.RandomSpatialGenerator;
import edu.umn.cs.spatialHadoop.ReadFile;
import edu.umn.cs.spatialHadoop.mapred.BlockFilter;
import edu.umn.cs.spatialHadoop.mapred.DefaultBlockFilter;
import edu.umn.cs.spatialHadoop.mapred.GridOutputFormat;
import edu.umn.cs.spatialHadoop.mapred.RTreeInputFormat;
import edu.umn.cs.spatialHadoop.mapred.RandomInputFormat;
import edu.umn.cs.spatialHadoop.mapred.ShapeInputFormat;
import edu.umn.cs.spatialHadoop.mapred.TextOutputFormat;
import edu.umn.cs.spatialHadoop.nasa.HDFPlot;
import edu.umn.cs.spatialHadoop.nasa.HDFToText;
import edu.umn.cs.spatialHadoop.nasa.MakeHDFVideo;
import edu.umn.cs.spatialHadoop.operations.RangeQuery;
import edu.umn.cs.spatialHadoop.operations.Repartition;
import edu.umn.cs.spatialHadoop.operations.RangeQuery.RangeFilter;
import edu.umn.cs.spatialHadoop.operations.RangeQuery.RangeQueryMap;
import edu.umn.cs.spatialHadoop.operations.RangeQuery.RangeQueryMapNoDupAvoidance;
import edu.umn.cs.spatialHadoop.operations.Repartition.RepartitionReduce;

import java.util.Iterator;

public class TSpatialHadoop {
	
	private static final Log LOG = LogFactory.getLog(RangeQuery.class);
	private FileSystem HDFS;
	private Configuration Config;

	public TSpatialHadoop(FileSystem hdfs, Configuration config) {
		HDFS = hdfs;
		Config = config;
	}
	
	public void CreateIndex(String inFile, String outFile, String indexType) throws IOException {	
		OperationsParams params = new OperationsParams(Config);
		params.set("sindex", indexType);
		params.set("shape", "rect");
		params.setBoolean("overwrite", true);
		params.setBoolean("local", false);
		Path InPath = new Path(inFile);
		Path OutPath = new Path(outFile);
		HDFS.delete(OutPath, true);
		
		try {
			Repartition.repartition(InPath, OutPath, params);
		} catch (java.io.IOException err) {
			System.err.println(err.getMessage());
		}
	}
	
	public long Query(double queryLeft, double queryRight, String indexPath, String indexType) throws IOException {
		OperationsParams params = new OperationsParams(Config, String.format("rect:%f,0,%f,1", queryLeft, queryRight));
		params.set("sindex", indexType);
		params.set("shape", "rect");
		params.setBoolean("overwrite", true);
		params.setBoolean("local", false);
		Path InPath = new Path(indexPath);
		Path OutPath = new Path("/user/ruslan/ruslan/sp_results");
		HDFS.delete(OutPath, true);
		long resultCount = RangeQuery.rangeQuery(InPath, OutPath, new Rectangle(queryLeft, 0, queryRight, 1), params);
		//upload results to a client machine
		FileStatus[] reduceFiles = HDFS.listStatus(new Path(OutPath.toString() + "/"));
		for (FileStatus status : reduceFiles) {
	        Path path = status.getPath();
	        if (path.toString().endsWith("_SUCCESS")) {
	        	continue;
	        }
	        FSDataInputStream file = HDFS.open(path);
	        //try {
		        HDFS.copyToLocalFile(path, new Path("temp.txt"));
		        FileSystem.getLocal(Config).delete(new Path("temp.txt"), true);
	        //} catch (IOException e) {
	        //	continue;
	        //}
	    }
		return resultCount;
	}
	
	public long QueryRTree(CellInfo[] queries, String indexPath, String indexType) throws IOException {

		OperationsParams params = new OperationsParams(Config, String.format("rect:%f,0,%f,1", 0.0, 1000.0));
		params.set("sindex", indexType);
		params.set("shape", "rect");
		
	    Path inFile = new Path(indexPath);
	    String outputFile = "/user/ruslan/ruslan/spatialhadoop.query";
	    Path outputPath = new Path(outputFile);
	    if (HDFS.exists(outputPath)) {
	    	HDFS.delete(outputPath, true);
	    }
	    
	    JobConf job = new JobConf(params, RangeQuery.class);
	    job.setJobName("RangeQuery");
	    
	    SpatialSite.setCells(job, queries);
	    
	    job.setClass(SpatialSite.FilterClass, dfs_interval_index.TSpatialHadoop.MultiRangeFilter.class, BlockFilter.class);

	    ClusterStatus clusterStatus = new JobClient(job).getClusterStatus();
	    job.setNumMapTasks(clusterStatus.getMaxMapTasks() * 5);
	    job.setNumReduceTasks(0);
	    
	    job.setMapOutputKeyClass(NullWritable.class);
	    Shape shape = new Rectangle();
	    job.setMapOutputValueClass(shape.getClass());
	    
	    //only RTree
	    job.setInputFormat(RTreeInputFormat.class);

	    // Decide which map function to use depending on how blocks are indexed
	    // And also which input format to use
//	    FileSystem inFs = (new Path(indexPath)).getFileSystem(job);
//	    if (SpatialSite.isRTree(inFs, new Path(indexPath))) {
//	    	job.setInputFormat(RTreeInputFormat.class);
//	    } else {
//	    	job.setInputFormat(ShapeInputFormat.class);
//	    }
	    
	    job.setMapperClass(dfs_interval_index.TSpatialHadoop.MultiRangeQueryMap.class);

	    
	    String selectedPartitions = new String();
	    int selected = 0;
	    {
	    	GlobalIndex<Partition> gIndex = SpatialSite.getGlobalIndex(inFile.getFileSystem(job), inFile);
			Iterator<Partition> it = gIndex.iterator();
			while (it.hasNext()) {
				Partition partition = it.next();
				
				for (int queryIndex = 0; queryIndex < queries.length; ++queryIndex) {
					if (partition.isIntersected(queries[queryIndex].getMBR())) {
						selectedPartitions += String.format("%f-%f;", partition.x1, partition.x2);
						++selected;
						break;
					}
				}
			}
	    }
	    System.out.println(String.format("Selected partitions: %d", selected));
	    job.set("partitions_use", selectedPartitions);
	    
//		GlobalIndex<Partition> gIndex = SpatialSite.getGlobalIndex(inFile.getFileSystem(job), inFile);	    
//	    if (gIndex != null && gIndex.isReplicated()) {
//	      job.setMapperClass(RangeQueryMap.class);
//	    } else {
//	      job.setMapperClass(RangeQueryMapNoDupAvoidance.class);
//	    }

	    job.setOutputFormat(TextOutputFormat.class);
	    ShapeInputFormat.setInputPaths(job, inFile);
	    TextOutputFormat.setOutputPath(job, outputPath);
	    
		String jarPath = dfs_interval_index.TSpatialHadoop.MultiRangeQueryMap.class.getProtectionDomain().getCodeSource().getLocation().getPath();
		if (jarPath.endsWith("jar")) {
			job.setJar(jarPath);
			System.out.println("JAR: " + jarPath);
		} else {
			//TODO: replace
			job.setJar("/home/arslan/src/1d_interval_index/src/hadoop/dfs_interval_index.jar");
		}
	    
		RunningJob runningJob = JobClient.runJob(job);
		Counters counters = runningJob.getCounters();
		Counter outputRecordCounter = counters.findCounter(TaskCounter.MAP_OUTPUT_RECORDS);
		final long resultCount = outputRecordCounter.getValue();
		
		FileStatus[] reduceFiles = HDFS.listStatus(new Path(outputFile + "/"));
		for (FileStatus status : reduceFiles) {
	        Path path = status.getPath();
	        if (path.toString().endsWith("_SUCCESS")) {
	        	continue;
	        }
	        FSDataInputStream file = HDFS.open(path);
	        //try {
		        HDFS.copyToLocalFile(path, new Path("temp.txt"));
		        FileSystem.getLocal(Config).delete(new Path("temp.txt"), true);
	        //} catch (IOException e) {
	        //	continue;
	        //}
	    }
		return resultCount;
	}
	
	
	  
	/**
	 * The map function used for multiple range query
	*/
	public static class MultiRangeQueryMap extends MapReduceBase implements
	      Mapper<Rectangle, Writable, NullWritable, Shape> {
	    /**A shape that is used to filter input*/
		private Shape queryShape;
		private Rectangle queryMbr;
		private CellInfo[] queries;
		
		@Override
		public void configure(JobConf job) {
		  super.configure(job);
		  try {
			  this.queries = SpatialSite.getCells(job);
		  } catch (IOException e) {
		  }
	  
		  queryShape = OperationsParams.getShape(job, "rect");
		  queryMbr = queryShape.getMBR();
		}
		
		private final NullWritable dummy = NullWritable.get();
		
		/**
		 * Map function for non-indexed blocks
		 */
		public void map(final Rectangle cellMbr, final Writable value,
		    final OutputCollector<NullWritable, Shape> output, Reporter reporter)
		        throws IOException {
		    RTree<Shape> shapes = (RTree<Shape>) value;
		    
		    for (int queryIndex = 0; queryIndex < this.queries.length; ++queryIndex) {
		    	this.queryMbr = queries[queryIndex].getMBR();
			    shapes.search(queryMbr, new ResultCollector<Shape>() {
				      @Override
				      public void collect(Shape shape) {
					        try {
					          boolean report_result = false;
					          if (cellMbr.isValid()) {
					            // Check for duplicate avoidance using reference point technique
					            Rectangle intersection = queryMbr.getIntersection(shape.getMBR());
					            report_result = cellMbr.contains(intersection.x1, intersection.y1);
					          } else {
					            // A heap block, report right away
					                report_result = true;
					              }
					              if (report_result)
					                output.collect(dummy, shape);
					            } catch (IOException e) {
					              e.printStackTrace();
					            }
					  }
			    });
		    }
	
		}
	};
	
	public static class MultiRangeFilter extends DefaultBlockFilter {
		    
		/**A shape that is used to filter input*/
		private String PartitionsUse;
		private Shape queryRange;
		
		@Override
		public void configure(JobConf job) {
		    this.PartitionsUse = job.get("partitions_use");
		    System.out.println(this.PartitionsUse);
		}
		
		@Override
		public void selectCells(GlobalIndex<Partition> gIndex,
		    ResultCollector<Partition> output) {
		  int numPartitions = 0;
		  if (gIndex.isReplicated()) {
		    // Need to process all partitions to perform duplicate avoidance
			Iterator<Partition> it = gIndex.iterator();
			while (it.hasNext()) {
				Partition shape = it.next();
				String key = String.format("%f-%f;", shape.x1, shape.x2);
				if (PartitionsUse.contains(key)) {
					++numPartitions;
					output.collect(shape);
				}
			}
		    //numPartitions = gIndex.rangeQuery(queryRange, output);
		    LOG.info("Selected "+numPartitions+" partitions overlapping ");
		  } else {
		    Rectangle queryRange = this.queryRange.getMBR();
		    // Need to process only partitions on the perimeter of the query range
		    // Partitions that are totally contained in query range should not be
		    // processed and should be copied to output directly
		    numPartitions = 0;
		    for (Partition p : gIndex) {
		      if (queryRange.contains(p)) {
		        // TODO partitions totally contained in query range should be copied
		        // to output directly
		
		        // XXX Until hard links are supported, R-tree blocks are processed
		        // similar to R+-tree
		        output.collect(p);
		        numPartitions++;
		      } else if (p.isIntersected(queryRange)) {
		        output.collect(p);
		        numPartitions++;
		      }
		    }
		    LOG.info("Selected "+numPartitions+" partitions on the perimeter of "+queryRange);
		  }
		}
	};
	
	public void Convert2MBR(double MIN_START, double MAX_START, String InPath, String OutPath) throws IOException {		
		JobConf job = new JobConf(Config);
		{//config
			job.setJobName("Convertor");
	
			//input-output paths
			FileInputFormat.setInputPaths(job, new Path(InPath));
			HDFS.delete(new Path(OutPath), true);
			FileOutputFormat.setOutputPath(job, new Path(OutPath));
			
			//jar
			String jarPath = dfs_interval_index.TSortMR.Map.class.getProtectionDomain().getCodeSource().getLocation().getPath();
			if (jarPath.endsWith("jar")) {
				job.setJar(jarPath);
				System.out.println("JAR: " + jarPath);
			} else {
				//TODO: replace
				job.setJar("/home/arslan/src/1d_interval_index/src/hadoop/dfs_interval_index.jar");
			}
			
			//output-input classes
			job.setInputFormat(TextInputFormat.class);
			job.setOutputFormat(GridOutputFormat.class);
			job.setMapperClass(dfs_interval_index.TSpatialHadoop.Map.class);
			job.setMapOutputKeyClass(IntWritable.class);
			job.setMapOutputValueClass(Shape.class);
			
			//ClusterStatus clusterStatus = new JobClient(job).getClusterStatus();
			//job.setNumMapTasks(10 * Math.max(1, clusterStatus.getMaxMapTasks()));
			//System.out.println(String.format("map tasks count: %d", clusterStatus.getMaxMapTasks()));
			job.setNumMapTasks(10);
			job.setNumReduceTasks(0);
	
			//TODO: set as a parameter
			Rectangle mbr = new Rectangle(MIN_START, 0, MAX_START, 1);
			CellInfo[] cells;
			{
				CellInfo cell = new CellInfo(1, mbr);
				cells = new CellInfo[] { cell };
				SpatialSite.setCells(job, cells);
			}
		}
		JobClient.runJob(job);

		FileStatus[] resultFiles = HDFS.listStatus(new Path(OutPath), new PathFilter() {
			@Override
			public boolean accept(Path path) {
				return path.getName().contains("_master");
			}
		});
		String ext = resultFiles[0].getPath().getName()
				.substring(resultFiles[0].getPath().getName().lastIndexOf('.'));
		Path masterPath = new Path(new Path(OutPath), "_master" + ext);
		
		OutputStream destOut = HDFS.create(masterPath);
		byte[] buffer = new byte[4096];
		for (FileStatus f : resultFiles) {
			InputStream in = HDFS.open(f.getPath());
			int bytes_read;
			do {
				bytes_read = in.read(buffer);
				if (bytes_read > 0)
					destOut.write(buffer, 0, bytes_read);
			} while (bytes_read > 0);
			in.close();
			HDFS.delete(f.getPath(), false);
		}
		destOut.close();

	}

	public static class Map extends MapReduceBase
								implements Mapper<LongWritable, Text, IntWritable, Rectangle> {
		/**List of cells used by the mapper*/
		private CellInfo[] cellInfos;

		/**Used to output intermediate records*/
		private IntWritable cellId = new IntWritable();

		@Override
		public void configure(JobConf job) {
			try {
				cellInfos = SpatialSite.getCells(job);
				super.configure(job);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		public void map(LongWritable key, Text value,
				OutputCollector<IntWritable, Rectangle> output, Reporter reporter)
						throws IOException {
			String[] chunks = value.toString().split(" ");
			Double start = Double.parseDouble(chunks[0]);
			Double end = start + Double.parseDouble(chunks[1]);
			Integer id = Integer.parseInt(chunks[2]);

			Rectangle shape = new Rectangle(start, 0, end, 1);
			for (int cellIndex = 0; cellIndex < cellInfos.length; cellIndex++) {
				if (cellInfos[cellIndex].isIntersected(shape.getMBR())) {
					cellId.set((int) cellInfos[cellIndex].cellId);
					output.collect(cellId, shape);
				}
			}
		}
		public void map(LongWritable key, Text value) throws IOException {
			String[] chunks = value.toString().split(" ");
			Double start = Double.parseDouble(chunks[0]);
			Double end = start + Double.parseDouble(chunks[1]);
			Integer id = Integer.parseInt(chunks[2]);
			Rectangle shape = new Rectangle(start, 0, end, 1);
			for (int cellIndex = 0; cellIndex < cellInfos.length; cellIndex++) {
				if (cellInfos[cellIndex].isIntersected(shape.getMBR())) {
					cellId.set((int) cellInfos[cellIndex].cellId);
					System.out.println(String.format("%d - %f", cellId.get(), shape.x1));
				}
			}
		}
	}

	
}
