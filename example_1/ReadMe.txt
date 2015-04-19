Problem Statement: Find top 10 average rated movies with descending order of rating (Use of chaining of multiple map-reduce job is a must here.)

How to execute?

Execute Map-Reduce:
hadoop jar homework_1_part_1.jar ${hdfs_path_to}/ratings.dat ${hdfs_path_to_output}

View Output:
hadoop fs -cat ${hdfs_path_to_output}/part_1_sorted_output/part-r-00000