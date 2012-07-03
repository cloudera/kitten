
SHELL_COMMAND = "ls -ltr >>"
CONTAINER_INSTANCES = 2

base_env = {
  -- PWD is specified by the test case.
  CLASSPATH = table.concat({"${CLASSPATH}", PWD .. "/target/classes/", PWD .. "/target/lib/*"}, ":"),
}

distshell = yarn {
  name = "Distributed Shell",
  timeout = 60000,
  memory = 256,

  master = {
    env = base_env,
    command = {
      base = "java -Xmx128m com.cloudera.kitten.appmaster.ApplicationMaster",
      args = { "-conf job.xml", "1> <LOG_DIR>/stdout 2> <LOG_DIR>/stderr" }, -- job.xml contains the client configuration info.
    }
  },

  container = {
    instances = CONTAINER_INSTANCES,
    env = base_env,
    command = SHELL_COMMAND .. TEST_FILE -- specified externally by the test case.
  }
}
