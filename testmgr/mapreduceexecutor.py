import os
from config import *

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
        
        

'''class JavaMapReduceExecutor:
    _hadoop_env_main_path = "/usr/local/hadoop/bin/hadoop "
    def execute(self, path_to_jar, java_file_name, hdfs_input_path, hdfs_output_path):
        command = (self._hadoop_env_main_path + "jar "
                   + path_to_jar + " "
                   + java_file_name + " "
                   + hdfs_input_path + " "
                   + hdfs_output_path)
        print(command)
        os.system(command)'''

class PythonMapReduceExecutor:
    def execute(self, path_to_mapper_code, path_to_reducer_code, hdfs_input_path, hdfs_output_path):
        mapper_file_name = path_to_mapper_code.split("/")[-1]
        reducer_file_name = path_to_reducer_code.split("/")[-1]
        command = ("hadoop jar "
                  + HADOOP_STREAMING_BASE_PATH + " "
                  + "-files " 
                  + path_to_mapper_code + ","
                  + path_to_reducer_code + " "
                  + "-mapper " + mapper_file_name + " "
                  + "-reducer " + reducer_file_name + " "
                  + "-input " + hdfs_input_path + " "
                  + "-output " + hdfs_output_path)
        print(command)
        os.system(command)
    
      
'''def test():
    obj = MapReduceExecutor()
    obj.execute("/home/hduser/Desktop/WordCountF/WordCountJ.jar",
                "WordCount", "/Input2", "Output3")

test()'''
    
                   
    
        
