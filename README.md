# Kitten: For Developers Who Like Playing with YARN

## Introduction

Kitten is a set of tools for writing and running applications on YARN,
the general-purpose resource scheduling framework that ships with Hadoop 2.0.0.
Kitten handles the boilerplate around configuring and launching YARN
containers, allowing developers to easily deploy distributed applications that
run under YARN.

This link provides a useful overview of what is required to create a new YARN
application, and should also help you understand our motivations in creating
Kitten.

http://hadoop.apache.org/common/docs/r0.23.0/hadoop-yarn/hadoop-yarn-site/WritingYarnApplications.html

## Build and Installation

To build Kitten, run:

	mvn clean install

from this directory. That will build the common, master, and client subprojects.

Kitten is developed against CDH4, which ships with an experimental YARN
module. Cloudera Manager [1] is the easiest way to get a Hadoop cluster with
YARN up and running.

The `java/examples/distshell` directory contains an example configuration file
that can be used to run the Kitten version of the Distributed Shell example
application that ships with Hadoop 2.0.0. To run the example, execute:

	hadoop jar kitten-client-0.1.0-jar-with-dependencies.jar distshell.lua distshell

where the jar file is in the `java/client/target` directory. You should also copy the
application master jar file from `java/master/target` to a directory where it can be
referenced from distshell.lua.

[1] https://ccp.cloudera.com/display/SUPPORT/Cloudera+Manager+Downloads

## Using Kitten

Kitten aims to handle the boilerplate aspects of configuring and launching YARN applications,
allowing developers to focus on the logic of their application and not the mechanics of how
to deploy it on a Hadoop cluster. It provides two related components that simplify
common YARN usage patterns:

1. A pair of Guava services, one for the client and one for the application master, that handle
all of the RPCs that are executed during the lifecycle of a YARN application, and
2. A configuration language, based on Lua 5.1 [1], that is used to specify the resources the
application needs from the cluster in order to run.

[1] http://www.lua.org/manual/5.1/

## Configuration DSL

Kitten makes extensive use of Lua's `table` data structure to organize information about how a
YARN application should be executed. Lua tables combine aspects of arrays and dictionaries into
a single data structure.

Kitten defines two additional functions in Lua that aid in configuring YARN applications.

### `yarn`

The `yarn` function is used to check that a Lua table that describes a YARN application contains all
of the information that Kitten will require in order to launch the application, as well as providing
some convenience functions that minimize how often some configuration information needs to be repeated.
Here is how yarn is used to define the distributed shell application:

	distshell = yarn {
	  name = "Distributed Shell",
	  timeout = 10000,
	  memory = 128,

	  master = {
 	    env = base_env, -- Defined elsewhere in the file
	    command = {
	      base = "java -Xmx128m com.cloudera.kitten.appmaster.ApplicationMaster",
	      args = { "-conf job.xml" }, -- job.xml contains the client configuration info.
	    }
	  },
	
	  container = {
	    instances = 3,
	    env = base_env,  -- Defined elsewhere in the file
	    command = "echo 'Hello World!' >> /tmp/hello_world"
	  }
	}

The `yarn` function checks for the following fields in the table that is passed to it, optionally
setting default values for optional fields that were not specified.

1. **name** (string, required): The name of this application.
2. **timeout** (integer, defaults to -1): How long the client should wait in milliseconds
before killing the application due to a timeout. If < 0, then the client will wait forever.
3. **user** (string, defaults to the user executing the client): The user to execute the
application as on the Hadoop cluster.
4. **queue** (string, defaults to ""): The queue to submit the job to, if the capacity scheduler
is enabled on the cluster.

In order to configure the application master and the container tasks, the `yarn` function checks for
the presence of a **master** field and either a **container** or **containers** field. The *container*
field is a shortcut for the case in which there is only one kind of container configuration; otherwise
the **containers** field expects a repeated list of container configurations. The **master** and
**container/containers** fields take a similar set of fields that specify how to allocate resources and
then run a command in the container that was created:

1. **conf** (table, optional): A table of key-value pairs that will be added to the Configuration
that is passed to the container via the job.xml file. The creation of this file is built-in to the Kitten
framework and is similar to how the MapReduce library uses the Configuration object to pass client-side
configuration information to tasks executing on the cluster.
2. **env** (table, optional): A table of key-value pairs that will be set as environment variables in the
container.
3. **memory** (integer, defaults to 128): The amount of memory to allocate for the container, in megabytes.
4. **instances** (integer, defaults to 1): The number of instances of this container type to create
on the cluster. Note that this only applies to the **container/containers** arguments; the system will only
allocate a single master for each application.
5. **priority** (integer, defaults to 0): The relative priority of the containers that are allocated. Note
that this prioritization is internal to each application; it does not control how many resources the
application is allowed to use or how they are prioritized.
6. **command/commands** (string(s) or table(s), optional): **command** is a shortcut for **commands** in the
case that there is only a single command that needs to be executed within each container. This field
can either be a string that will be run as-is, or it may be a table that contains two subfields: a **base**
field that is a string and an **args** field that is a table. Kitten will construct a command by concatenating
the values in the args table to the base string to form the command to execute.
7. **resources** (table of tables, optional): The resources (in terms of files, URLs, etc.) that the command
needs to run in the container. This is a table that is composed of other tables, similar to an array. The
names of these fields are described below.

### `cat`

The `cat` function takes a table `base` as its only argument and returns a reference to a function
that will copy the key-value pairs of `base` into any table given to it unless that table already
has a value defined for a key that occurs in `base`. An example will probably help clarify the usage.

	base = cat { x = 1, y = 2, z = 3 }  -- 'base' is a reference to a function, it is not a table.
	derived = base { x = 17, y = 29 }   -- 'derived' also has the key-value pair 'z=3' defined in it as well.
	base_as_table = base {}             -- 'base_as_table' is a table, not a function.

It is common to have some arguments or environment variables in a YARN application that need to be
shared between the application master and the tasks that are launched in containers. `cat` provides a convenient
way to reference this shared information within the configuration file while allowing some extra parameters to
be specified:

	-- Define the common environment variables once.
	base_env = cat { CLASSPATH = "...", JAVA_HOME = "..." }
	
	my_app = yarn {
	  master = {
	    -- Copy base_env and add an extra setting for the master.
	    env = base_env { IS_MASTER = 1 },
	  }

	  container = {
	    -- Copy base_env and add an extra variable for the container nodes.
	    env = base_env { IS_MASTER = 0 },
	  }
	}

