import os
from .config import *


def java_map_reduce_execute(path_to_jar, java_file_name, hdfs_input_path, hdfs_output_path):
    hadoop_env_main_path = "/usr/local/hadoop/bin/hadoop "
    full_command = (hadoop_env_main_path + "jar "
                    + path_to_jar + " "
                    + java_file_name + " "
                    + hdfs_input_path + " "
                    + hdfs_output_path)
    try:
        os.system(full_command)
        print("java map reduce execution successful")
        return True
    except Exception as e:
        print(e)
        print("Error executing java map reduce code")
        return False


'''def test():
    obj = MapReduceExecutor()
    obj.execute("/home/hduser/Desktop/WordCountF/WordCountJ.jar",
                "WordCount", "/Input2", "Output3")

test()'''
