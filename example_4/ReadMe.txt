Problem Statement: Find top 10 average rated "Action" movies with descending order of rating. (This is in-memory map side join. Put the smallest file in the memory. If any movie has multiple genres and you find one of its genre is Action then consider the movie genre is Action)

How to execute?

Execute Map-Reduce:
hadoop jar homework_2_part_1.jar assignement_2.homework_2_part_1 ${hdfs_path_to_input_files} ${hdfs_path_to_output} 

View Output:
hadoop fs -cat ${hdfs_path_to_output}/part-r-00000