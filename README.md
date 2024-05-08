# Full Lifecycle Data Analysis on a Large-scale and Leadership Supercomputer: What Can We Learn from It?

## 1 Getting Started Instructions

This paper presents a comprehensive and detailed analysis of the system and job characteristics on a cutting-edge computing system, Sunway TaihuLight supercomputer. Our study offers valuable insights into trends in I/O performance, as well as the challenges and opportunities for improvement in high-performance computing environments. We present our methodology, findings, and the significance of our study. Analysis scripts, data, and figures can be accessed in Section 2. These results have significant implications for the design, optimization, and management of HPC systems, as well as the development of more efficient data-intensive applicaitons. Additionally, we discuss the implications of our findings for future research and practice in this area.
This project will show our data processing and analyzing scripts, as well as data collected by Beacon (An open-source and lightweight collecting tool). 
Details see blow.

## 2 Detailed Instructions

### 2.1 About Beacon and Data.

Beacon collects all data used in this paper on the Sunway TaihuLight. These data include computing nodes, forwarding nodes, storage nodes, and job-running information. We have no authority to open the access or provide a simulator to access the Beacon or all these data. However, Beacon's code and part of the collected data (The data is processed to hide the user's information) can be accessed through the link blew: https://github.com/Beaconsys/Beacon.

### 2.2 processing scripts

The "Pre-processing" directory contains Python scripts that are responsible for extracting data from Beacon's database and performing necessary pre-processing procedures. Details can be seen in the README file.

### 2.2 Json_pretreament/*.py
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

## 2.3 Experiment_result

The "Experiment_results" directory showcases data processing scripts, data files, and generated images corresponding to each experiment presented in this study. The ".m" files represent data processing scripts that can be executed directly through Matlab, the ".mat" files represent the data utilized during processing, and the resulting images are shared in PDF format. Our work in this paper can be divided into the following four parts:
\begin{enumerate}
\item The first part analyzes the load on the compute nodes, examining the load variation over time, the behavior of users submitting jobs over the day, etc. The results can be found in /Experiment results/Comp_load_analysis, which corresponds to Figures 3 to 7 in this paper.
\item The second part analyzes the load and anomalies of the system nodes and investigates the uneven load phenomenon and fault anomalies in the system, and the results can be found in /Experiment results/Sys_load_analysis, which corresponds to Figures 8 to 12 in this paper.
\item The third part analyzes the job-running information, examining the average parallelism, running time, and waiting time of jobs, and the results can be found in /Experiment results/Job_runnig_information, corresponding to Figures 13 to 14 in this paper.
\item The fourth part analyzes the I/O behavior of the job, and the study is carried out in terms of the changing trend of the I/O pattern of the job, the io library used, the overall I/O performance of the job, and the file access characteristics, etc. The results can be found in /Experiment results/Job_io_performance, which correspond to Figure 15 to Figure 19 in this paper.

### 2.1 Comp_load_analysis
 - The first part analyzes the load on the compute nodes, examining the load variation over time, the behavior of users submitting jobs over the day, etc. The results can be found in /Experiment_results/Comp_load_analysis, corresponding to Figures 3 to 7 in this paper.

### 2.2 Sys_load_analysis
 - The second part analyzes the system nodes' load and anomalies and investigates the system's load imbalance phenomenon and fault anomaly trends. The results can be found in /Experiment_results/Sys_load_analysis, corresponding to Figures 8 to 12 in this paper.

### 2.3 Job_runnig_information
 - The third part analyzes the job-running information and shows the average job parallelism, job running time, job waiting time, etc. The results can be found in /Experiment_results/Job_runnig_information, corresponding to Figures 13 to 14 in this paper.

### 2.4 Job_io_performance
 - The fourth part analyzes the I/O behavior of the job, and the study is carried out in terms of the changing trend of the job's I/O pattern, the io library that jobs used, the overall job I/O performance, and the file access patterns. The results can be found in /Experiment_results/Job_io_performance, corresponding to Figure 15 to Figure 19 in this paper.
