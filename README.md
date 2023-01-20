
## Introduction
  This folder (i.e., ./TrajectorySimCal, /SimCalSW, /SimCalGRSW) holds the source codes in our paper, Ghost: A General Framework for High-Performance Online Similarity Queries over Distributed Trajectory Streams


## Environment Preparation
  - Flink version: 1.3.2
  - A cluster containing 15 nodes, where each node is equipped with two 12-core processors (Intel Xeon E-5-2620 v3 2.40 GHz), 128GB RAM
  - System version: Ubuntu 14.04.3
  - Java version: 1.8.0
  - Please refer to the source code to install all required packages of libs by Maven.


## Dataset Description
  - "./TrajectorySimCal/src/main/resources/traData" contains one of the tested datasets (Brinkhoff)
  - The format of the tested dataset is:
         Moving object ID, observed timestamp, location of x, location of y
  
  Note: The synthetic dataset (Brinkhoff) can refer to http://iapg.jade-hs.de/personen/brinkhoff/generator/
  

## Running 
  - Select one of the three projects (SimCalSW, SimCalGRSW or TrajectorySimCal);
  - Import the selected project to Intellij IDEA；
  - Downloading all required dependences by Maven; 
  - Set the configuration file (“./PROJECT_NAME/src/main/resources/default.properties"), including DataSet_Path, SINK_DIR, SOCKET_HOST, QUERY_PATH, similarity computation related parameters, and clustering parameters;
  - Run “nc -l 6543” to open the socket for query input if needed;
  - Package the project to a X.jar, where X is your project name；
  - Load the packaged X.jar to the master node of your Flink cluster；
  - Running your project in a cluster environment.
 