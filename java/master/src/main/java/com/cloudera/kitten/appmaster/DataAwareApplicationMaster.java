package com.cloudera.kitten.appmaster;

import com.cloudera.kitten.appmaster.params.lua.LuaApplicationMasterParameters;
import com.cloudera.kitten.appmaster.service.DataAwareApplicationMasterServiceImpl;

public class DataAwareApplicationMaster extends ApplicationMaster {

  @Override
  public int run(String[] args) throws Exception {
    ApplicationMasterParameters params = new LuaApplicationMasterParameters(getConf());
    ApplicationMasterService service = new DataAwareApplicationMasterServiceImpl(params);
    
    service.startAndWait();
    while (service.isRunning()) {
      Thread.sleep(1000);
    }
    
    System.exit(0);
    return 0;
  }
  
}
