import os

class MapReduceCodeCompiler:
    _hadoop_common_jar_path = """/usr/local/hadoop/share/hadoop/common/hadoop-common-2.7.2.jar"""
    _hadoop_client_core_jar_path = """/usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-core-2.7.2.jar"""
    _hadoop_client_common_cli_jar_path = """/usr/local/hadoop/share/hadoop/common/lib/commons-cli-1.2.jar"""
    _base_command = "javac -classpath"
    def compile_code(self, path):
        destination_class_files_path = ("/".join(path.split("/")[:-1]) 
                                        + "/ClassFiles")
        os.mkdir(destination_class_files_path)
        command = (self._base_command + " "
                   + self._hadoop_common_jar_path + ":"
                   + self._hadoop_client_core_jar_path + ":"
                   + self._hadoop_client_common_cli_jar_path
                   + " -d " + destination_class_files_path
                   + " " + path)
        os.system(command)


'''def test():
    obj = MapReduceCodeCompiler()
    obj.compile_code("/home/hduser/Desktop/WordCountF/WordCount.java")
 
test()'''
