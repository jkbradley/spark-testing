"""
Test script for Decision Trees and Random Forests

Run with a range of parameters.
Use DecisionTreeRunner, which prints parameters, accuracy, and timing results to STDOUT.
The output file is named: "dts-[testTag]-[date-time].log"
"""

import os, re, time

# Test settings
testTag = "avoid-agg"
dataset = "mnist"
maxDepths = [5, 10]
numTreess = [1, 10]
fracTest = 0.2
logdir = "/root/mnist-logs"

dieOnFailure = True
dryRun = True  # print what will run, but do not run anything

# Datasets
if dataset == "mnist":
    trainFilePath = "s3n://databricks-mllib/mnist8m-libsvm"
    testFilePath = "s3n://databricks-meng/mllib-data/mnist-digits/mnist-digits-test.txt"
else:
    trainFilePath = dataset

# Run stuff
sparkDir = "/root/spark"

def getMasterURI():
    f = open(sparkDir + "/conf/spark-env.sh")
    re_pattern = re.compile("export SPARK_MASTER_IP=(.+)")
    masterURI = ""
    for line in f:
        line = line.strip()
        match = re_pattern.match(line)
        if match:
            masterURI = match.group(1)
            break
    f.close()
    return masterURI

masterURI = getMasterURI()

for maxDepth in maxDepths:
    for numTrees in numTreess:
        timeStamp = time.strftime("%Y-%m-%d_%H:%M:%S")
        outPath = "dts-%s-maxDepth%d-numTrees%d-%s.log" % (testTag, maxDepth, numTrees, timeStamp)
        runString = "time /root/spark/bin/spark-submit --class org.apache.spark.examples.mllib.DecisionTreeRunner" \
          + " --master spark://" + masterURI + ":7077 --driver-memory 20g" \
          + " /root/spark-git/examples/target/scala-2.10/spark-examples-1.2.0-SNAPSHOT-hadoop1.0.4.jar" \
          + (" %s --testInput %s" % (trainFilePath, testFilePath)) \
          + (" --maxDepth %d --numTrees %d --fracTest %g" % (maxDepth, numTrees, fracTest)) \
          + (" &> %s" % outPath)
        print "RUNNING: " + runString
        if not dryRun and os.system(runString) != 0:
            print "FAILED TEST: " + runString
            try:
                os.rename(outPath, outPath + ".FAILED")
                print "RENAMED FAILED TEST LOG: " + outPath + ".FAILED"
            except:
                print "COULD NOT RENAME FAILED TEST LOG: " + outPath
            if dieOnFailure:
                raise Exception("Failed during test.")

