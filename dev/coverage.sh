#!/bin/sh

cd "$(dirname "$0")"

cd ..

sbt jacoco && open target/scala-2.12/jacoco/report/html/index.html
