# Analysis_on_TaihuLight
## 1 Pre_processing
This dictory contains Python scripts that are responsible for extracting data from Beacon's database and performing necessary pre-processing procedures.

### 1.1 Json_pretreament/*.py
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

### 1.2 Get_comp_load.py
 - The program will process the overall information of compute node, and obtain the abnormal job statistics, job information of jobs submitted in different time periods.

### 1.3 Get_fwd_load.py:
 - The program processes fowarding nodes data to hourly data to show

### 1.4 Get_ost_load.py:
 - The program processes OSTs data to hourly data to show

### 1.5 Get_app_IO_perf.py
 - The program is used to obtain the applicaitons I/O performance from the Beacon database. It is also responsible for clustering them according to I/O performance characteristics, and then saves the data.

### 1.6 Handle_file/*.py
 - redisfile.py: \
    handle Analysed secondly data to find jobs queue which access the same file

 - scan.py:\
    scan mysql and find jobs

 - handle_filequeue.py:\
    scan the share files and get the job queue distribution and job access files distribution

 - get_ip.py
    convert the job using compute node list to ip list

### 1.7 Benchmark/*
 - Two C language programs under the folder are codes for parallel read and write operations on files in n-1 mode, and run.sh is a shell script file for compiling and submitting the two programs.

## 2 Experiment_result
The dictory directory showcases data processing scripts, data files, and generated images corresponding to each experiment presented in this study, includes four components:

### 2.1 Comp_load_analysis
 - The first part analyzes the load on the compute nodes, examining the load variation over time, the behavior of users submitting jobs over the day, etc. The results can be found in /Experiment_results/Comp_load_analysis, corresponding to Figures 3 to 7 in this paper.

### 2.2 Sys_load_analysis
 - The second part analyzes the system nodes' load and anomalies and investigates the system's load imbalance phenomenon and fault anomaly trends. The results can be found in /Experiment_results/Sys_load_analysis, corresponding to Figures 8 to 12 in this paper.

### 2.3 Job_runnig_information
 - The third part analyzes the job-running information and shows the average job parallelism, job running time, job waiting time, etc. The results can be found in /Experiment_results/Job_runnig_information, corresponding to Figures 13 to 14 in this paper.

### 2.4 Job_io_performance
 - The fourth part analyzes the I/O behavior of the job, and the study is carried out in terms of the changing trend of the job's I/O pattern, the io library that jobs used, the overall job I/O performance, and the file access patterns. The results can be found in /Experiment_results/Job_io_performance, corresponding to Figure 15 to Figure 19 in this paper.
