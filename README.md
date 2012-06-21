# Kitten: For Developers Who Like Playing with YARN

## Introduction

Kitten is a set of tools for writing and running applications on YARN,
the general-purpose resource scheduling framework that ships with Hadoop 2.0.0.
Kitten handles the boilerplate around configuring and launching YARN
containers, allowing developers to easily deploy distributed applications that
run under YARN.

## Build and Installation

To build Kitten, run:

	mvn clean install

from this directory. That will build the common, master, and client subprojects.

Kitten is developed against CDH4, which ships with an experimental YARN
module. Cloudera Manager [1] is the easiest way to get a Hadoop cluster with
YARN up and running.

The `java/examples/distshell` directory contains an example configuration file
that can be used to run the Kitten version of the distributed shell example
application that ships with Hadoop 2.0.0. To run it, you execute:

	hadoop jar kitten-client-0.1.0-jar-with-dependencies.jar distshell.lua distshell

where the jar file is in the `java/client/target` directory. You should also copy the
application master jar file from `java/master/target` to a directory where it can be
referenced from distshell.lua.

[1] https://ccp.cloudera.com/display/SUPPORT/Cloudera+Manager+Downloads

## Configuration DSL

Coming soon. :)
