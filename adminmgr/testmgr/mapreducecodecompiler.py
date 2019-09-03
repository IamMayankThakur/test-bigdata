import os


def java_map_reduce_compile(path_to_code, destination_path):
    #hadoop_common_jar_path = """/usr/local/hadoop/share/hadoop/common/hadoop-common-3.2.0.jar"""
    #hadoop_client_core_jar_path = """/usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-core-3.2.0.jar"""
    #hadoop_client_common_cli_jar_path = """/usr/local/hadoop/share/hadoop/common/lib/commons-cli-1.2.jar"""
    base_command = "javac"
    
    #hack_path_to_code = "/home/hduser/Documents/H11.java"

    destination_class_path = destination_path + "/ClassFiles"
    try:
        os.mkdir(destination_path + "/ClassFiles")
        print("Created classfiles directory")
    except Exception as e:
        print("Error creating directory")
        print(str(e))
    '''full_command = (base_command + " "
                    + hadoop_common_jar_path + ":"
                    + hadoop_client_core_jar_path + ":"
                    + hadoop_client_common_cli_jar_path
                    + " -d " + destination_class_path
                    + " " + hack_path_to_code)'''
    full_command = (base_command + " "
                    +  path_to_code + " " + "-d " + destination_class_path + " "
                    + "-cp" + " $(hadoop classpath)")
    try:
        print(full_command)
        os.system(full_command)
    except Exception as e:
        print("Error compiling java code")
        print(str(e))
        return False
    return True
