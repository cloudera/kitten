
SHELL_COMMAND = "ls -ltr >>"
CONTAINER_INSTANCES = 2

base_env = {
  -- PWD is specified by the test case.
  CLASSPATH = table.concat({"${CLASSPATH}", PWD .. "/target/classes/", PWD .. "/target/lib/*"}, ":"),
}

distshell = yarn {
  name = "Distributed Shell",
  timeout = 10000,
  memory = 128,

  master = {
    env = base_env,
    command = {
      base = "java -Xmx128m com.cloudera.kitten.appmaster.ApplicationMaster",
      args = { "-conf job.xml" }, -- job.xml contains the client configuration info.
    }
  },

  container = {
    instances = CONTAINER_INSTANCES,
    env = base_env,
    resources = { job_jar },
    command = SHELL_COMMAND .. TEST_FILE -- specified externally by the test case.
  }
}
