import os
from .config import *

from hdfs import InsecureClient

def java_map_reduce_execute(path_to_jar, java_file_name, hdfs_input_path, hdfs_output_path):
    hadoop_env_main_path = "/usr/local/hadoop/bin/hadoop "
    full_command = (hadoop_env_main_path + "jar "
                    + path_to_jar + " "
                    + java_file_name + " "
                    + hdfs_input_path + " "
                    + hdfs_output_path)
    try:
        x = os.system(full_command)
        print("COMMAND: " + full_command)
    except Exception as e:
        print(e)
        print("Error executing java map reduce code")
        return False
    if x != 0:
        try:
            client = InsecureClient(('http://'
                                    + HADOOP_HOST_NAME + ':'
                                    + HADOOP_NAMENODE_PORT_NUMBER), user=HADOOP_USER_NAME)
        except Exception as e:
            print(e)
            print("Error connecting to hdfs client")
            return False
        try:
            client.delete(hdfs_output_path, recursive=True)
        except Exception as e:
            print(e)
            print("Error deleting output directory")
            return False
        print("Error executing java map reduce code")
        return False
    print("java map reduce execution successful")
    return True


def python_map_reduce_execute(path_to_mapper, path_to_reducer, hdfs_input_path, hdfs_output_path):
    command = "hadoop" + " " + "jar" + " " 
    command += HADOOP_STREAMING_BASE_PATH + " "
    command += "-D" + " " + "mapred.map.tasks=1" + " "
    command += "-files" + " "
    command += path_to_mapper + ","
    command += path_to_reducer + " "
    command += "-mapper" + " " + path_to_mapper.split("/")[-1] + " "
    command += "-reducer" + " " + path_to_reducer.split("/")[-1] + " "
    command += "-input" + " " + hdfs_input_path + " "
    command += "-output" + " " + hdfs_output_path
    
    print(command)
    print(os.system("pwd"))
    try:
        x = os.system(command)
    except Exception as e:
        print(e)
        print("Error executing python map reduce code")
        return False
    if x != 0:
        try:
            client = InsecureClient(('http://'
                                    + HADOOP_HOST_NAME + ':'
                                    + HADOOP_NAMENODE_PORT_NUMBER), user=HADOOP_USER_NAME)
        except Exception as e:
            print(e)
            print("Error connecting to hdfs client")
            return False
        try:
            client.delete(hdfs_output_path, recursive=True)
        except Exception as e:
            print(e)
            print("Error deleting output directory")
            return False
        print("Error executing python map reduce code")
        return False
    
    print("Python map reduce execution successful")
    return True
    

'''def test():
    obj = MapReduceExecutor()
    obj.execute("/home/hduser/Desktop/WordCountF/WordCountJ.jar",
                "WordCount", "/Input2", "Output3")

test()'''
