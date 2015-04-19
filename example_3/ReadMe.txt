Problem Statement: Given some genre, find all the movies belong to that genre.

How to execute?

Execute Map-Reduce:
hadoop jar homework_1_part_3.jar ${hdfs_path_to}/movies.dat ${hdfs_path_to_output} Comedy:Romance

View Output:
hadoop fs -cat ${hdfs_path_to_output}/part-r-00000
