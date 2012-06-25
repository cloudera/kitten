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

	base_env = cat { CLASSPATH = "...", JAVA_HOME = "..." }
	
	my_app = yarn {
	  master = {
	    env = base_env { IS_MASTER = 1 },
	  }

	  container = {
	    env = base_env { IS_MASTER = 0 },
	  }
	}

