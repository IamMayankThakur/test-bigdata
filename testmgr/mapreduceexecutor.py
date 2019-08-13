import os

class MapReduceExecutor:
    _hadoop_env_main_path = "/usr/local/hadoop/bin/hadoop "
    def execute(self, path_to_jar, java_file_name, hdfs_input_path, hdfs_output_path):
        command = (self._hadoop_env_main_path + "jar "
                   + path_to_jar + " "
                   + java_file_name + " "
                   + hdfs_input_path + " "
                   + hdfs_output_path)
        print(command)
        os.system(command)
       
'''def test():
    obj = MapReduceExecutor()
    obj.execute("/home/hduser/Desktop/WordCountF/WordCountJ.jar",
                "WordCount", "/Input2", "Output3")

test()'''
    
                   
    
        
