-- An example Kitten configuration file for the distributed shell app, running on
-- CDH4.
--
-- To execute, run:
--
-- hadoop jar kitten-client-0.2.0-jar-with-dependencies.jar distshell.lua distshell
--
-- from a directory that contains the client jar, the master jar, and distshell.lua.
--
-- You can also play with the SHELL_COMMAND, CONTAINER_INSTANCES, and
-- MASTER_JAR_LOCATION arguments as need be.

-- The command to execute.
SHELL_COMMAND = "ls -ltr >> /tmp/kitten_dir_contents"
-- The number of containers to run it on.
CONTAINER_INSTANCES = 2
-- The location of the jar file containing kitten's default ApplicationMaster
-- implementation.
MASTER_JAR_LOCATION = "kitten-master-0.2.0-jar-with-dependencies.jar"

-- CLASSPATH setup.
H_SHARE = "/usr/lib/hadoop"
COMMON_CP = table.concat({H_SHARE, "/*:", H_SHARE, "/lib/*"}, "")
HDFS_CP = table.concat({H_SHARE, "-hdfs/*:", H_SHARE, "-hdfs/lib/*"}, "")
YARN_CP = table.concat({H_SHARE, "-yarn/*:", H_SHARE, "-yarn/lib/*"}, "")

-- Resource and environment setup.
base_resources = {
  ["master.jar"] = { file = MASTER_JAR_LOCATION }
}
base_env = {
  CLASSPATH = table.concat({"${CLASSPATH}", COMMON_CP, HDFS_CP, YARN_CP, "./master.jar"}, ":"),
}

-- The actual distributed shell job.
distshell = yarn {
  name = "Distributed Shell",
  timeout = 10000,
  memory = 512,

  master = {
    env = base_env,
    resources = base_resources,
    command = {
      base = "${JAVA_HOME}/bin/java -Xms64m -Xmx128m com.cloudera.kitten.appmaster.ApplicationMaster",
      args = { "-conf job.xml", "1> <LOG_DIR>/stdout", "2> <LOG_DIR>/stderr" },
    }
  },

  container = {
    instances = CONTAINER_INSTANCES,
    env = base_env,
    command = SHELL_COMMAND
  }
}
