package com.cloudera.kitten.appmaster.util;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class HDFSFileFinderTest {

  private static final Log LOG = LogFactory.getLog(HDFSFileFinderTest.class);

  protected static MiniDFSCluster cluster = null;
  protected static DistributedFileSystem fs = null;
  protected static Configuration conf = new Configuration();
  protected static int numDataNodes = 5;
  protected static int replicationFactor = 3;
  protected static long blockSize = 8; // should be power of 2
  
  @BeforeClass
  public static void setup() throws InterruptedException, IOException {
    LOG.info("Starting up MR cluster");
    if (cluster == null) {
      conf.set("dfs.namenode.replication.min", "" + replicationFactor);
      conf.set("dfs.block.size", "" + blockSize);
      conf.set("io.bytes.per.checksum", "" + 4);
      cluster = new MiniDFSCluster
          .Builder(conf)
          .numDataNodes(numDataNodes)
          .build();
      fs = cluster.getFileSystem();
    }
  }

  @AfterClass
  public static void tearDown() throws IOException {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }
  
  @Test
  public void testFileFinder() throws Exception {
    // make a file
    File tmpFile = File.createTempFile("test-file", ".txt");
    // add some data
    PrintWriter pw = new PrintWriter(tmpFile); pw.println("test file data"); pw.flush(); pw.close();
    tmpFile.deleteOnExit();
    
    // copy to hdfs
    Path src = new Path(tmpFile.getAbsolutePath());
    Path dst = new Path("test-file.txt");
    fs.copyFromLocalFile(src, dst);
    
    // get the hosts
    Map<String, Long> bytesHeld = HDFSFileFinder.getNumBytesOfGlobHeldByDatanodes(dst, conf);
    
    for (String node : bytesHeld.keySet())
      LOG.info(node + " : " + bytesHeld.get(node));
    
    assertTrue(replicationFactor <= bytesHeld.keySet().size());
    
  }
}