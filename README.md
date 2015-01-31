interval_index
==============

The work on the project is definitely not finished, so you may find the distribution to be quite cluttered. 

The main contribution is in two files:
- src/interval_index/interval_index.hpp
- src/hadoop/dfs_interval_index/src/dfs_interval_index/TDFSIntervaIndex.java 
Those are in-memory (C++) and HDFS implementation of CIntIA (Checkpint Interval Index Array).

Everything around it was create to test the indices and compare them to other solutions.

But if you want to use it, you can do the following:

1) for in-memory C++ tests you need a g++-4.8 (almost certainly you have it). 
Just run the script in the root folder:

sh build_and_run_inmemory.sh

It will build a binary for tests, create datasets for testing the data structure performance under different 
query lengths, and start experiments. It will compare CIntIA with R-tree, Interval Tree, Segment Tree and NClist.

All results will be in a folder called 'tests_results'.

2) for HDFS implementation you need a Hadoop cluster with Spatial Hadoop 2.2 libraries on every cluster 
machine in any one directory that is in the list "hadoop classpath". 

You need to put there spatialhadoop-2.2.jar and spatialhadoop-jts-1.8.jar. 

The jar file with CIntIA is the src/hadoop/dfs_interval_index.jar
You can run it in this way:

java -classpath &lt;hadoop_libs&gt;*:&lt;spatial_hadoop_libs&gt;*:dfs_interval_index.jar  dfs_interval_index.TTests &lt;path with hadoop configs&gt; &lt;some directory in HDFS&gt; 

where:
&lt;hadoop_libs&gt; - path with hadoop jars, I recommend to do smth like this: 

mkdir libs;  cp `hadoop classpath |  sed  "s/:/jar /g"` libs/

&lt;spatial_hadoop_libs&gt; - path to Spatial Hadoop 2.2 libs

&lt;path with hadoop configs&gt; - path where you keep your core-site.xml, hdfs-site.xml, mapred-site.xml and yarn-site.xml

&lt;some directory in HDFS&gt; - some path in you HDFS (hdfs://&lt;bla-bla&gt;) where will be saved all source files and indices.

You better run this from a machine which has a wired connection to the cluster, or even a cluster machine, 
since otherwise the index construction might take a lot of time. 

If everything was fine, the program will generate datasets, then indices (for CIntIA and Spatial Hadoop), 
and after that run query tests for CIntIA, Spatial Hadoop and Map Reduce. 

Further details soon.






