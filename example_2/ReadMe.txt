Problem Statement: List each genre of movie and count the movies of each genre.

How to execute?

Execute Map-Reduce:
hadoop jar homework_1_part_2.jar ${hdfs_path_to}/movies.dat ${hdfs_path_to_output}

View Output:
hadoop fs -cat ${hdfs_path_to_output}/part-r-00000
