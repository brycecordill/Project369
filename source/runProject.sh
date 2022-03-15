#!/usr/bin/env bash

# Run in main project directory

# Need to set variables to correct path in App
# Need to move data to /user/$USER/input/data
# Need to delete /user/$USER/output

sbt package
spark-submit --class example.App --master yarn target/scala-2.11/project369_2.11-0.1.jar "/user/$USER/input" "/user/$USER/output"