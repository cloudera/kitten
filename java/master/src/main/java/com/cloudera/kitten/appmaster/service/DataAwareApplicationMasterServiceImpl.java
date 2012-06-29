package com.cloudera.kitten.appmaster.service;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;

import com.cloudera.kitten.ContainerLaunchContextFactory;
import com.cloudera.kitten.ContainerLaunchParameters;
import com.cloudera.kitten.appmaster.ApplicationMasterParameters;
import com.cloudera.kitten.appmaster.util.HDFSFileFinder;

public class DataAwareApplicationMasterServiceImpl extends
    ApplicationMasterServiceImpl {

  public DataAwareApplicationMasterServiceImpl(
      ApplicationMasterParameters params) {
    super(params);
  }
  
  
  @Override
  protected void startUp() {
    super.resourceManager = resourceManagerFactory.connect();
    
    RegisterApplicationMasterResponse registration = null;
    try {
      registration = resourceManager.registerApplicationMaster(createRegistrationRequest());
    } catch (YarnRemoteException e) {
      LOG.error("Exception thrown registering application master", e);
      stop();
      return;
    }
    
    this.containerLaunchContextFactory = new ContainerLaunchContextFactory(
        registration.getMinimumResourceCapability(), registration.getMaximumResourceCapability());
    
    List<ContainerLaunchParameters> containerParameters = parameters.getContainerLaunchParameters();
    
    HDFSFileFinder ff = new HDFSFileFinder();
    
    // simple data-aware logic
    //   1. look at first HDFS resource
    //   2. find the host with the most
    //   3. set the desired host
    //
    // TODO: only decides based on the first file, could be much smarter
    
    for (ContainerLaunchParameters params : containerParameters) {
      if (params.getHDFSResources().isEmpty()) {
        continue;
      }
      else {
        Path p = params.getHDFSResources().get(0);
        String hostessWithTheMostess = "*";
        try {
          Map<String, Long> numBytes = ff.getNumBytesOfGlobHeldByDatanodes(p);
          long most = -1L;
          for (String k : numBytes.keySet()) {
            long b = numBytes.get(k);
            if (b > most) {
              most = b;
              hostessWithTheMostess = k;
            }
          }
        } catch (IOException e) {
          LOG.error("Trouble communicating with HDFS. Defaulting to any host for this container.");
          hostessWithTheMostess = "*";
        }
        params.setDesiredHostname(hostessWithTheMostess);
      }
    }
    
    if (containerParameters.isEmpty()) {
      LOG.warn("No container configurations specified");
      stop();
      return;
    }
    for (int i = 0; i < containerParameters.size(); i++) {
      ContainerLaunchParameters clp = containerParameters.get(i);
      containerTrackers.add(new ContainerTracker(clp));
    }
  }

}
