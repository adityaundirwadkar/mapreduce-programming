Problem Statement:  List all the movies with its genre where the movie genre is Action or Drama and the average movie rating is in between 4.4 - 4.7 and only the male users rate the movie. (This is reduce side join).

How to execute?

Execute Map-Reduce:
hadoop jar homework_2_part_2.jar assignment_2.homework_2_part_2 ${hdfs_path_to_input_files} ${hdfs_path_to_output} 

View Output:
hadoop fs -cat ${hdfs_path_to_output}/part-r-00000