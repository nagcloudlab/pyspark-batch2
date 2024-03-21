

- **Partitioning** is the process of dividing the data into smaller chunks based on some column or condition.




Why spark using partitions ?


# 1. **Parallelism** is the ability to perform multiple operations simultaneously.

    -> Partitions allow Spark to distribute data across multiple nodes in a cluster, enabling parallel processing

# 2 **Scalability** is the ability to handle a growing amount of work by adding resources to the system.

    -> Partitions allow Spark to scale out to handle larger datasets

# 3 **Fault Tolerance** is the ability to recover from failure.

    -> Partitions allow Spark to recover from failure by re-computing the lost partition    

    note: RDD - Resilient Distributed Dataset

# 4 **Performance** is the ability to execute a task quickly.
    
    -> Partitions allow Spark to optimize performance by reducing the amount of data that needs to be processed

# 5 **Memory Management** is the ability to manage memory efficiently.
    
    -> Partitions allow Spark to manage memory more efficiently by reducing the amount of data that needs to be stored in memory

------------------------------------------------------------------------------------------------    

re-partitioning: 

    -> The process of redistributing the data across the partitions is called re-partitioning.
    
    
why we nedd re-partitioning ?

    -> To optimize the performance of the Spark job by reducing the amount of data that needs to be processed.
    -> to reduce data skewness
    -> Otimize resource utilization
    -> to reduce the time to process the data


------------------------------------------------------------------------------------------------
How to re-partition the data ?

    -> repartition() method
    -> coalesce() method

------------------------------------------------------------------------------------------------    


.repartition()  

    - increase or decrease the number of partitions
    - it shuffles the data across the cluster, costly in terms of performance
      but is necessary when the data is skewed or when the number of partitions is not optimal

------------------------------------------------------------------------------------------------


.coalesce()

    - decrease the number of partitions
    - it does not shuffle the data across the cluster, it only combines partitions
    - it is less costly in terms of performance than repartitioning

------------------------------------------------------------------------------------------------
