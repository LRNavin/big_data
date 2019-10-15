## Lab 2: Supercomputing for Big Data, Delft University of Technology
### Group 6 - Navin Raj Prabhu - 4764722, Anwesh Marwade - 5052068

#### October 13, 2019


## Preliminary Run

After deploying our JAR on the AWS cluster (configuration: 1 Master node and 20 worker nodes - c4.8xlarge
specification), we decided to do some initial code runs to analyse our performance and more importantly to
preemptively measure execution time. This allowed us to reasonably evaluate the scalability of our code before
running it on the entire dataset. We first looked at some aspects we could tweak straightaway. 

### RDD vs Dataframe

We executed our code on data samples using both the implementations from lab one i.e. RDD and Dataframe
while familiarizing ourselves with the AWS cluster environment as well. We measured execution times for 1
segment (15 minute data), 1 day, 1 month, 3 months and 6 months on our cluster. As expected from our
analysis in the previous lab, RDD was promising for very small sized data but fell well behind the Dataframe
implementation as we increased the number of data segments, see figure below.

<p align="center">
<img src="https://github.com/LRNavin/big_data/blob/master/images/RDDvDataset.png" width="500" height="300" />
</p>

```
Figure 1: Analysing performance RDD vs Dataframe
```
Based on our observations (as expected) the Dataframe implementation seems to be the optimal choice for
this analysis on the cluster.

### Analysing Preliminary results

After choosing the dataframe implementation based on our initial run, we made some minor changes to the
code from lab 1. For creating the packaged .JAR, we replaced the code for printing the output, with code
to flush the output to a log file on S3. While searching for any evident optimisations that we could leverage
by tweaking the code, we looked at Kryo Serializer, using rank function instead of UDF(s) and utilising the
distributed environment of AWS.

## Full Dataset Run

### Cluster Setup

After some preliminary tests with smaller datasets, we were confident enough to scale our code to the full dataset
(4TB). For this task, we decided to use the Dataframe/Dataset based approach. For the cluster setup, we used
20 EC2 (c4.8xlarge) cluster slave-nodes and 1 EC2 (c4.8xlarge) cluster master-node. Number of executors, by
default, was set to 1 per node, so in total, 20 executors. 

![Initial Error](https://github.com/LRNavin/big_data/blob/master/images/20ex/init_error.png)
```
Figure 2: Full Dataset Error
```
This cluster setup worked well for smaller GDELT datasets, i.e. For a day, For a month and For a year.
But, it failed for the whole datasets (4TB). When checking logs, the following error occurred (seen in Fig-2).
The error was related to Resource Insufficiency - in specific, memory limit error. So, we decided to check cluster
configurations to tackle this error. On research, we found a way to configure the EMR cluster to increase
resource allocation to the cluster nodes. In AWS EMR cluster, it is possible to configure your executors to
utilize the maximum resources possible on each node in a cluster by using the spark configuration classification
to setmaximizeResourceAllocationoption to true. This EMR-specific option calculates the maximum compute
and memory resources available for an executor on an instance in the core instance group. It then sets the
corresponding spark-defaults settings based on this information. For Example - Creating one executor per Node
giving him most of the Nodes resources. In our case, 20 executors for our 20 nodes.

### Results

![Optimised Run](https://github.com/LRNavin/big_data/blob/master/images/20ex/result_20ex.png)
```
Figure 3: Full Dataset run - Results
```
After configuringmaximizeResourceAllocation, we were ready to run our dataframe-based code on the full
dataset. The results of the run can be seen in Fig- 3. The complete 4TB of GDELT dataset was analysed
for the task of top 10 task for the day, in just 5.7 minutes. With this result, we achieved the most basic
requirement of this lab assignment - to run the full dataset in less than 30 min. Nevertheless, with the hope to
improve this performance and to look out for any exisitng drawbacks of the cluster setup, we decided to analyse
the execution statistics.


### Performance Analysis

The next step is to check the how our cluster’s resources get utilized during the full dataset run. For this task,
we used the Ganglia. Such an analysis, would also help us optimise the cost of the run, by detecting places for
improvements where resources are under used. 

![Cluster Performance](https://github.com/LRNavin/big_data/blob/master/images/20ex/20ex_full.png)
```
Figure 4: Cluster Performance Statistics - c4.8xlarge [20 nodes, 20 executors]
```

Some important inferences using Ganglia, from Fig- 4,

1. CPU utilization (Fig- 4a, 4b) : We see that the cluster utilizes only 70% of the CPU, but the cpu load is
    evenly distributed across all the 20 nodes. This is one place where an optimisation could be made. This
    could be improved if we increase the number of tasks that can run on each node.
2. RAM usage (Fig- 4c) : The memory usage graph shows no anomalous behaviour. Probably, if we optimize
    the CPU usage, better cache and usage in RAM can be expected.
3. Load (Fig- 4d) : A similar inference as CPU usage can be drawn here. That is, while total CPU’s capacity
    - loads/processes is 756, along the run only ̃500 loads/processes (which is 70% of the total) is utilised.

Overall, we can say that optimizing CPU usage is one main point of bother and fixing that might improve
cluster performance and eventually the run-time and the run-cost. Optimising CPU usage will also improve
RAM usage and Load, as CPU usage directly both RAM and Load.

### Cost of the run

The current cost of the run is,

- Run-Cost = (0.41 * 21) * (5.7/60) = **0.82$**
- where, Cost per Node = 0.41$, Total Node used = 20, Total Run-time = 5.7 minutes.

## Fine tuning and optimisations

### Code Optimisations

We explored some code optimisations with a goal of improving our execution time and distributing our
computation optimally.

#### Rank vs UDF

<p align="center">
<img src="https://github.com/LRNavin/big_data/blob/master/images/rankvudf.png" width="500" height="300" />
</p>

```
Figure 5: Optimising the code: Rank vs User-defined
```
Using the Rank function to filter out the top ten topics, we did not find a considerable improvement in execution time (As seen in Fig-5).
We saw that our UDF, which used __sort__ and __take__ functions to get the top ten, showed better results. So,
we chose to keep our filtering approach from the previous lab.

#### Kryo Serialization

We tried to optimise our shuffling operations in the RDD implementation by using the kryo serialization for a
more lightweight serialization. However, we ended up carrying out our analysis using the dataset implementation
which inherently uses a specialized encoder for serialization during network transmission or processing [(link)](https://spark.apache.org/docs/2.1.0/sql-programming-guide.html). The
optimized performance of the dataset API was verified during our inital run.

#### Storing the results to S3 (Parallel writes)

We changed the code to allow writing the output to a file on S3. By doing so we avoided the’take/show’
operation which asked the master node to gather the data in order to print it to the output. By logging our
output to S3, we try to leverage the _distributed-ness_ of S3.

### Cloud Optimisations

In this section, the tasks of fine-tuning the cluster’s configuration will be discussed.

#### Spark configurations
Exploring configurations for optimally running the spark application on our cluster, we found out that the two
main resources Spark (and YARN) worry about are _CPU_ and _memory_. Since the CPU usage was a bottleneck
(seen from previous sections), we decided to increase parallel computations in nodes by tuning certain YARN
configurations like configuring the number of executors per node. The documentation talks mainly about the
following configurations,

``` −−num−executors ?−−executor−cores ?−−executor−memory?```

Based on multiple blogs we found that the optimal value for–executor-cores, i.e. number of concurrent tasks
per executor is 5. A value greater than 5 is known to plateau in performance and give a bad show. Next, based
on our cluster size and specifications, we see that we have 36 vCores per c4.8xLarge node, which gives us a total
of 720 (36 * 20) cores and additionally considering 2 threads per core, we double that value. Taking 5 cores
for each executor (concurrent tasks), we tried setting the–num-executorsvalue to (720 * 2)/5≈300. For the
memory allocations, we calculated 60GB (Memory spec for c4.8xlarge) available for 15 executors per node (
executors for 20 nodes) and allocated 60/15≈4GB for each executor. Attempting to run the analysis with the
said configuration, we managed to improve the CPU utilisation by about 20 per-cent! (Performance visualized in Fig-6) We could have further optimised these numbers if not for the limited credits available. Nevertheless, post optimisation, the complete 4TB of GDELT dataset was analysed for the task of top 10 task for the day, in just **4.6 minutes**.

``` −−num−executors 320−−executor−cores 5−−executor−memory 4g−−driver−memory 10G```

![Cluster Performance - Optimised](https://github.com/LRNavin/big_data/blob/master/images/20ex/320ex_full.png)
```
Figure 6: Cluster Performance Statistics - c4.8xlarge [20 nodes, 320 executors]
```

##### Optimised Results
* Run-Time - **4.6 min**
* Run-Cost - **0.66 $**
* Price per Performance - **0.17 $/TB**

##### Improvements
* Faster Run-time
* Cheaper Run cost
* Better CPU usage **>80%**
* Optimal Lads/Procs

#### Best EMR configuration

We use the __Price per Performance ($/TB)__ metric to measure the utility of the EMR cluster’s configurations.
Table - 1, are some of the configurations tested.

![EMR Configurations Performance](https://github.com/LRNavin/big_data/blob/master/images/20ex/table_result.png)
```
Table 1: Performance of Cluster - Different setups
```
Firstly we tested the default configuration(as per lab manual) and tried to tweak the number of executors
with the hope of achieving better CPU utilization (which seemed to be initially stuck at 60-70 per-cent) knowing
for sure that it would contribute in reducing the run-time and correspondingly the price for performance metric.
Eventually we managed to increase the CPU utilization by optimising the cluster resource configuration which
reduced our run-time by more than a minute and showed the expected improvement in the Price for performance
metric. We also ran our configuration using better spec machines (c5.9xlarge) and saw that it further reduced
the run-time. We saw that by tuning our spark job such that it optimises its resource allocation, we reduced
the Run-costs and improved the performance metric.

## Future scope

Even though we optimised the code to get a runtime of less than 30 minutes, there can be more changes made to
the code that might result in performance improvements. For example, various serialization libraries could be
tested out, garbage generation/collection could be improved, one shuffling operation could be further reduced. These
improvements would need to be regression tested for evaluating its utility versus performance (improvement).
Further, the multitude to configurations available in spark could be tested out further and tweak accordingly
to try to reduce the run-time by optimising hardware utilisation.

## References

https://spark.apache.org/docs/2.1.0/sql-programming-guide.html

