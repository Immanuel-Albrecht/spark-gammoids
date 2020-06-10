#!/bin/bash

cd $(dirname $0)

cd ..

sbt package
sbt assembly

spark-shell --jars target/scala-2.12/*assembly*.jar -i dev/init.scala "$@"
