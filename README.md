# Nap
Nap: Network-Aware Data Partitions for Efficient Distributed Processing

Hadoop modification for the _network_, nodes' downlinks, for a mutliway join example.

## Table of Contents
* [Introduction](#Introduction)
* [Technologies](#technologies)
* [Features](#Features)
* [Usage](#Usage)

* [Sources](#Sources)
* [Contact](#Contact)

## Introduction
It is well known that in many Distributed Processing the __network__ influnce graetly the task finish time, and it could even be the bottleneck of the task or the larger job.
 __Apache Hadoop__ which is widely accpepted framework in the Big Data industry does not take into account the _network_ topology, unaware of it when running a job. There is a basic assumption that the processing is performed with a homogeneous computers, but when they are heterogeneous Hadoop is slower. Thus, we have been _modifying Hadoop_ for a better partition of the data in the network, and thus shotrter job's completion time. 
 
## Features
+ An Hadoop 2.9.1 network aware compiled version (without the source code), the output from compiling project [@Nap-Hadoop-2.9.1](https://github.com/razo7/Nap-Hadoop-2.9.1), (hadoop).
+ Apache License 2.0 (LICENSE)
+ git lfs file (.gitattributes)
+ RSA key for EC2 (ec2-proj2.pem)
+ Testing downlink and uplink between nodes (downlinkSpeed-test.sh or scp-speed-test.sh or speedtest-cli) and the average of them using
``` bash dd1.sh slave1 slave2 slave3 slave4 slave5 #run for checking network connectivity by sending a small files (50 MB or larger) while collecting the transfer rate ``` (bwTest)
+ Input- Two txt files for WordCount and the rest is a three tables structure (input).
+  (mav-had)
 ## Technologies
* Hadoop 2.9.1
* Java 8
* Python 2.7
* Wolfram Mathematica 11.0
* Git 2.20.1
Ubuntu, Maven.
## Usage

### How to Multiway Join Three Tables With and Without _Network Awareness_?
Multiway join (Java code) example of three tables, Papers, Papers-Authors, and Authors.

### How to Run a Native Hadoop Cluster?
How to run a project? Does a project has minimum hardware requirements?

### How to Collect the Job's Parameters and Make Figures?
How to parse the job's information from the job history server (_REST API_) and plot the plots as in the article.


## Features
## Usage (Optional)
## Documentation (Optional)
## Tests (Optional)

- Going into more detail on code and technologies used
- I utilized this nifty <a href="https://github.com/adam-p/markdown-here/wiki/Markdown-Cheatsheet" target="_blank">Markdown Cheatsheet</a> for this sample `README`.

---
## Sources
+  Alec Jacobson alecjacobsonATgmailDOTcom [@scp-speed-test.sh](https://www.alecjacobson.com/weblog/?p=635)
+  Nap-Hadoop-2.9.1 [@repisotory](https://github.com/razo7/Nap-Hadoop-2.9.1) with the Hadoop source code and the network aware changes 

## Contact
Created by Or Raz (razo7) as part of my master's thesis work and my [@article](IEEE.com) - feel free to contact me on Linkedin [@Or Raz](https://www.linkedin.com/in/or-raz/) or [@Email](razo@post.bgu.ac.il)!
