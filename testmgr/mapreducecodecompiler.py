import os

def java_map_reduce_compile(path_to_code, destination_path):
    hadoop_common_jar_path = """/usr/local/hadoop/share/hadoop/common/hadoop-common-2.7.2.jar"""
    hadoop_client_core_jar_path = """/usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-core-2.7.2.jar"""
    hadoop_client_common_cli_jar_path = """/usr/local/hadoop/share/hadoop/common/lib/commons-cli-1.2.jar"""
    base_command = "javac -classpath"
    
    destination_class_path = destination_path + "/ClassFiles"
    try:
        os.mkdir(destination_path + "/ClassFiles")
        print("Created classfiles directory")
    except Exception as e:
        print("Error creating directory")
        print(str(e))
    full_command = (base_command + " "
                    + hadoop_common_jar_path + ":"
                    + hadoop_client_core_jar_path + ":"
                    + hadoop_client_common_cli_jar_path
                    + " -d " + destination_class_path
                    + " " + path_to_code)
    try:
        os.system(full_command)
    except Exception as e:
        print("Error compiling java code")
        print(str(e))
        return False
    return True
