# Full Lifecycle Data Analysis on a Large-scale and Leadership Supercomputer: What Can We Learn from It?

## 1 Getting Started Instructions

This paper presents a comprehensive and detailed analysis of the system and job characteristics on a cutting-edge computing system, Sunway TaihuLight supercomputer. Our study offers valuable insights into trends in I/O performance, as well as the challenges and opportunities for improvement in high-performance computing environments. We present our methodology, findings, and the significance of our study. These results have significant implications for the design, optimization, and management of HPC systems, as well as the development of more efficient data-intensive applicaitons. Additionally, we discuss the implications of our findings for future research and practice in this area. This project will show our data processing and analyzing scripts, as well as data collected by Beacon (An open-source and lightweight collecting tool). Details see blow.

## 2 Detailed Instructions

### 2.1 About Beacon and Data.

Beacon collects all data used in this paper on the Sunway TaihuLight. These data include computing nodes, forwarding nodes, storage nodes, and job-running information. We have no authority to open the access or provide a simulator to access the Beacon or all these data. However, Beacon's code and part of the collected data (The data is processed to hide the user's information) can be accessed through the link blew: https://github.com/Beaconsys/Beacon.

### 2.2 processing and analysis scripts

The "processing" directory contains Python scripts that are responsible for extracting data from Beacon's database and performing necessary pre-processing procedures.

#### 2.2.1 Json_pretreament/*.py
 - interface.py:\
    the interface to handle Json to Analysed secondly data (for lwfs_client(format_type = new) ,lustre_server) \
    @param st: startdate (datetime '%Y-%m-%d') \
    @param et: enddate (datetime '%Y-%m-%d) \
    @param json_type: type of data (ost1 represent luser_server, comp represent lwfs_client)

 - lustre_client_parse.py:\
    the interface to handle Json to Analysed secondly data (for lustre_client) \
    @param st: startdate (datetime '%Y-%m-%d') \
    @param et: enddate (datetime '%Y-%m-%d)

 - lwfs_client_parse.py:\
    the interface to handle Json to Analysed secondly data (for lustre_client(format_type = old)) \
    @param st: startdate (datetime '%Y-%m-%d') \
    @param et: enddate (datetime '%Y-%m-%d) \

 - config.py:\
    record Json path and Analysed secondly data path

 - lustre_server.py:\
    the functions to parse luster_server Json

 - lustre_client.py:\
    the functions to parse luster_client Json

 - lwfs_client.py:\
    the functions to parse lwfs_client Json

 - utils.py:\
    the method functions

#### 2.2.2 Get_comp_load.py
 - The program will process the overall information of compute node, and obtain the abnormal job statistics, job information of jobs submitted in different time periods.

#### 2.2.3 Get_fwd_load.py:
 - The program processes fowarding nodes data to hourly data to show

#### 2.2.4 Get_ost_load.py:
 - The program processes OSTs data to hourly data to show

#### 2.2.5 Get_app_IO_perf.py
 - The program is used to obtain the applicaitons I/O performance from the Beacon database. It is also responsible for clustering them according to I/O performance characteristics, and then saves the data.

#### 2.2.6 Handle_file/*.py
 - redisfile.py: \
    handle Analysed secondly data to find jobs queue which access the same file

 - scan.py:\
    scan mysql and find jobs

 - handle_filequeue.py:\
    scan the share files and get the job queue distribution and job access files distribution

 - get_ip.py
    convert the job using compute node list to ip list

#### 2.2.7 Benchmark/*
 - Two C language programs under the folder are codes for parallel read and write operations on files in n-1 mode, and run.sh is a shell script file for compiling and submitting the two programs.

#### 2.2.8 Simulation for job scheduler