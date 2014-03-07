# Kitten: For Developers Who Like Playing with YARN

## Introduction

Kitten is a set of tools for writing and running applications on YARN,
the general-purpose resource scheduling framework that ships with Hadoop 2.2.0.
Kitten handles the boilerplate around configuring and launching YARN
containers, allowing developers to easily deploy distributed applications that
run under YARN.

This link provides a useful overview of what is required to create a new YARN
application, and should also help you understand the motivation for creating
Kitten.

http://hadoop.apache.org/docs/stable/hadoop-yarn/hadoop-yarn-site/WritingYarnApplications.html

## Build and Installation

To build Kitten, run:

	mvn clean install

from this directory. That will build the common, master, and client subprojects.

The `java/examples/distshell` directory contains an example configuration file
that can be used to run the Kitten version of the Distributed Shell example
application that ships with Hadoop 2.2.0. To run the example, execute:

	hadoop jar kitten-client-0.2.0-jar-with-dependencies.jar distshell.lua distshell

where the jar file is in the `java/client/target` directory. You should also copy the
application master jar file from `java/master/target` to a directory where it can be
referenced from distshell.lua.

## Using Kitten

Kitten aims to handle the boilerplate aspects of configuring and launching YARN applications,
allowing developers to focus on the logic of their application and not the mechanics of how
to deploy it on a Hadoop cluster. It provides two related components that simplify
common YARN usage patterns:

1. A configuration language, based on [Lua 5.1](http://www.lua.org/manual/5.1/), that is used to specify the resources the
application needs from the cluster in order to run.
2. A pair of Guava services, one for the client and one for the application master, that handle
all of the RPCs that are executed during the lifecycle of a YARN application, and

## Configuration Language

Kitten makes extensive use of Lua's [table type](http://lua-users.org/wiki/TablesTutorial) to organize information about how a
YARN application should be executed. Lua tables combine aspects of arrays and dictionaries into
a single data structure:

	a = { "this", "is", "a", "lua", "table" }
	
	b = {
	  and = "so",
	  is = "this",
	  ["a.key.with.dots"] = "is allowed using special syntax"
	}

### The `yarn` Function

The `yarn` function is used to check that a Lua table that describes a YARN application contains all
of the information that Kitten will require in order to launch the application, as well as providing
some convenience functions that minimize how often some configuration information needs to be repeated.
Here is how yarn is used to define the distributed shell application:

	distshell = yarn {
	  name = "Distributed Shell",
	  timeout = 10000,
	  memory = 512,

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
5. **conf** (table, optional): A table of key-value pairs that will be added to the `Configuration` instance
that is passed to the launched containers via the job.xml file. The creation of job.xml is built-in to the Kitten
framework and is similar to how the MapReduce library uses the Configuration object to pass client-side
configuration information to tasks executing on the cluster.

In order to configure the application master and the container tasks, the `yarn` function checks for
the presence of a **master** field and either a **container** or **containers** field. The *container*
field is a shortcut for the case in which there is only one kind of container configuration; otherwise
the **containers** field expects a repeated list of container configurations. The **master** and
**container/containers** fields take a similar set of fields that specify how to allocate resources and
then run a command in the container that was created:

1. **env** (table, optional): A table of key-value pairs that will be set as environment variables in the
container. Note that if all of the environment variables are the same for the master and container, you
can specify the **env** table once in the yarn table and it will be linked to the subtables by the `yarn`
function.
2. **memory** (integer, defaults to 512): The amount of memory to allocate for the container, in megabytes.
If the same amount of memory is allocated for both the master and the containers, you can specify the value
once inside of the yarn table and it will be linked to the subtables by the `yarn` function.
3. **cores** (integer, defaults to 1): The number of virtual cores to allocate for the container. If the
same number of cores are allocated for both the master and the containers, you can specify the value once
inside of the yarn table and it will be linked to the subtables by the `yarn` function.
4. **instances** (integer, defaults to 1): The number of instances of this container type to create
on the cluster. Note that this only applies to the **container/containers** arguments; the system will only
allocate a single master for each application.
5. **priority** (integer, defaults to 0): The relative priority of the containers that are allocated. Note
that this prioritization is internal to each application; it does not control how many resources the
application is allowed to use or how they are prioritized.
6. **tolerated_failures** (integer, defaults to 4): This field is only specified on the application master,
and it specifies how many container failures should be tolerated before the application shuts down.
7. **command/commands** (string(s) or table(s), optional): **command** is a shortcut for **commands** in the
case that there is only a single command that needs to be executed within each container. This field
can either be a string that will be run as-is, or it may be a table that contains two subfields: a **base**
field that is a string and an **args** field that is a table. Kitten will construct a command by concatenating
the values in the args table to the base string to form the command to execute.
8. **resources** (table of tables, optional): The resources (in terms of files, URLs, etc.) that the command
needs to run in the container. An outline of the resources fields are given in the following section.

YARN has a mechanism for copying files that are needed by an application to a working directory created
for the container that the application will run in. These files are referred to in Kitten as **resources.**
A resource specification might look like this:

	master = {
	  ...
	  resources = {
	    ["app.jar"] = {
	      file = "/localfs/path/to/local/jar/long-name-for-app.jar",
	      type = "file",               -- other value: 'archive'
	      visibility = "application",  -- other values: 'private', 'public'
            },

	    { hdfs = "/hdfs/path/to/cluster/file/example.txt" },
	  }
	}

In this specification, we are referencing a jar file that is stored on the local filesystem, and by
setting the key to "app.jar", we indicate that the file should be named "app.jar" when it is copied into
the container's working directory. Kitten handles the process of copying the local file to HDFS before
the job begins so that the file is available to the application. The other resource we are referencing
is already stored on HDFS, which is indicated by the use of the **hdfs** field instead of the **file**
field in the table. Since we did not specify a name for this resource via a key in the table, Kitten
uses the file's name (example.txt) as the name for the resource in the working directory.

### The `cat` Function

The `cat` function takes a table `base` as its only argument and returns a reference to a function
that will copy the key-value pairs of `base` into any table given to it unless that table already
has a value defined for a key that occurs in `base`. An example will probably help clarify the usage.

	base = cat { x = 1, y = 2, z = 3 }  -- 'base' is a reference to a function, not a table.
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

## Kitten Services

Kitten provides a pair of services that handle all interactions with YARN's ResourceManager: one for the client
[YarnClientService](http://github.com/cloudera/kitten/blob/master/java/client/src/main/java/com/cloudera/kitten/client/YarnClientService.java)
and one for the application master [ApplicationMasterService](http://github.com/cloudera/kitten/blob/master/java/master/src/main/java/com/cloudera/kitten/appmaster/ApplicationMasterService.java). These services are implemented
via the [Service API](http://docs.guava-libraries.googlecode.com/git/javadoc/com/google/common/util/concurrent/Service.html)
 that is defined in Google's Guava library for Java and manage the cycle of starting a new application,
monitoring it while it runs, and then shutting it down and performing any necessary cleanup when the application
has finished. Additionally, they provide auxillary functions for handling common tasks during application execution.

The client and master service APIs have a similar design. They both rely on an interface that specifies how to configure
various requests that are issued to YARN's ResourceManager. In the case of the client, this is the [YarnClientParameters](http://github.com/cloudera/kitten/blob/master/java/client/src/main/java/com/cloudera/kitten/client/YarnClientParameters.java)
interface, and for the master, it is the [ApplicationMasterParameters](http://github.com/cloudera/kitten/blob/master/java/master/src/main/java/com/cloudera/kitten/appmaster/ApplicationMasterParameters.java)
interface. Kitten ships with implementations of these interfaces that get their values from a combination of the Kitten Lua DSL
and an optional map of key-value pairs that are specified in Java and may be used to provide configuration information that is not
known until runtime. For example, this map is used to communicate the hostname and port of the master
application to the slave nodes that are launched via containers.

### Client Services

Kitten ships with a default client application, [KittenClient](http://github.com/cloudera/kitten/blob/master/java/client/src/main/java/com/cloudera/kitten/client/KittenClient.java),
which is intended to be used with YARN applications that provide their status via a tracking URL and do not require application-specific client interactions. KittenClient
has two required arguments: the first should be a path to a .lua file that contains the configuration information for a YARN application, and the second should be the
name of one of the tables defined in that file using the `yarn` function. Additionally, KittenClient implements Hadoop's `Tool` interface, and so those two required
arguments may be preceded by the other Hadoop configuration arguments.

KittenClient can also be used as a basis for your own YARN applications by subclassing KittenClient and overriding the `handle` method for interacting with the
service.

### ApplicationMaster Services

Kitten also ships with a default application master, the aptly-named [ApplicationMaster](http://github.com/cloudera/kitten/blob/master/java/master/src/main/java/com/cloudera/kitten/appmaster/ApplicationMaster.java),
which is primarily intended as an example.

Most real YARN applications will also incorporate some logic for coordinating the slave nodes into their application master
binary, as well as for passing additional configuration information to the Lua DSL. The ApplicationMasterParameters interface provides methods
that allow the application master to specify the hostname (`setHostname`), port (`setClientPort`), and a tracking URL (`setTrackingUrl`) that
will be passed along to the ResourceManager and then to the client.

## FAQ

1.  Why Lua as a configuration language?

    Lua's original use case was as a tool for configuring C++ applications, and it has been widely adopted in the gaming community as
    a simple scripting language. It has a number of desirable properties for the use case of configuring YARN applications, namely:

    1. **It integrates well with both Java and C++.** We expect to see YARN applications written in both languages, and expect that
    Kitten will need to support both. Having a single configuration format for both languages reduces the cognitive overhead for developers.
    2. **It is a programming language, but not much of one.** Lua provides a complete programming environment when you need it, but
    mainly stays out of your way and lets you focus on configuration.
    3. **It tolerates missing values well.** It is easy to reference values in a configuration file that may not be defined until much
    later. For example, we can specify parameters that will eventually contain the value of the master's hostname and port, but are
    undefined when the client application is initially configured.

    That said, we fully expect that other languages (e.g., Lisp) would make excellent configuration languages for YARN applications, which
    is why the `YarnClientParameters` and `ApplicationMasterParameters` are interfaces: we can swap out other configuration DSLs that
    may make more sense for certain developers or use cases.

2.  What are your plans for Kitten?

    That's a good question. In the short term, we're primarily interested in writing YARN applications that leverage Kitten, which will give
    us a chance to fix bugs and add solutions to common design patterns. We expect that Kitten will add functionality over time that makes
    it easier to handle failures, report internal application state, and provide for dynamically allocating new resources over time. We also
    expect to spend a fair amount of time adding C++ versions of the client and application master services so that Kitten's DSL could also
    be used to configure C++ applications to run on YARN.

    Additionally, we could add functionality for configuring all kinds of jobs- like MapReduces, Pig scripts, Hive queries, etc.- as
    Kitten tasks, using functions similar to `yarn`. We could also use Kitten to specify DAGs of tasks and treat Kitten as an alternative
    way of interacting with Oozie's job scheduling functionality.

3.  Were any animals harmed in the development of this library?

    No.

4.  What if I have questions that aren't answered by this (otherwise awesome) FAQ?

    We have mailing lists where you can ask questions, either as a [user of Kitten](https://groups.google.com/a/cloudera.org/forum/#!forum/kitten-user) or as a [developer of Kitten](https://groups.google.com/a/cloudera.org/forum/#!forum/kitten-dev).
