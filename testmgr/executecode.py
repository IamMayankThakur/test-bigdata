import os
from hdfs import InsecureClient
from mapreducecodecompiler import java_map_reduce_compile
from jarfilegenerator import java_jar_file_generator
from mapreduceexecutor import java_map_reduce_execute
from mapreduceexecutor import PythonMapReduceExecutor
from testoutput import TestOutput
from config import *

def download_file(hdfs_output_path, path, test_case_number):
    try:
        client = InsecureClient(('http://'
                                + HADOOP_HOST_NAME + ':' 
                                + HADOOP_NAMENODE_PORT_NUMBER), user=HADOOP_USER_NAME)
    except:
        print("Error connecting to hdfs client")
        return
    try:
        client.download(HADOOP_OUTPUT_PATH + "/", os.path.join(path, test_case_number))
    except:
        print("Error download output file")
        return
    try:
        client.delete(HADOOP_OUTPUT_PATH, recursive=True)	
    except:
        print("Error deleting output directory")
        return
    
def process_java(path_to_code, team_name):
    team_folder_path = os.path.join(TEAMS_BASE_PATH, team_name)
    try:
        os.mkdir(team_folder_path)
        print("Created team folder")
    except Exception as e:
        print("Error creating team folder")
        print(e)
    if(java_map_reduce_compile(path_to_code, team_folder_path) == False):
        return None
    print("[TEST-COMPONENT-LOG]["+team_name+"] COMPILATION SUCCESSFUL")
    
    if(java_jar_file_generator(os.path.join(team_folder_path, "ClassFiles"),
                               team_folder_path, "WordCountJ") == False):
        return None        
    print("[TEST-COMPONENT-LOG]["+"/"+team_name+"] JAR FILE CREATED")
    return team_folder_path
    
def java_execute_test_cases(path_to_code, team_name):
    team_folder_path = process_java(path_to_code, team_name)
    if(team_folder_path == None):
        print("Processing java code failed")
        return False
    output_paths = []
    for test_case in range(1,TEST_CASES+1):
        output_paths.append(execute_java(path_to_code, team_folder_path, str(test_case)))
    return output_paths
    
def python_execute_test_cases(path_to_mapper, 
                              path_to_reducer, team_name):
    output_paths = []
    for test_case in range(1,TEST_CASES+1):
        output_paths.append(execute_python(path_to_mapper,
                                                path_to_reducer,
                                                team_name,
                                                str(test_case)))
        return output_paths

def execute_java(path_to_code, team_folder_path, test_case_number):
    if(java_map_reduce_execute(os.path.join(team_folder_path, "WordCountJ.jar"),
                               "WordCount", "/Test_Case_" + str(test_case_number), 
                               HADOOP_OUTPUT_PATH) == False):
        return None
    print("[TEST-COMPONENT-LOG]MAP REDUCE EXECUTION SUCCESSFUL")
    
    download_file(HADOOP_OUTPUT_PATH, team_folder_path, test_case_number)
    return os.path.join(team_folder_path, test_case_number, "part-r-00000")      
        
def execute_python(path_to_mapper, path_to_reducer, team_name, test_case_number):
    team_folder_path = create_team_folder(team_name)
    obj_1 = PythonMapReduceExecutor()
    obj_1.execute(path_to_mapper, path_to_reducer, "/Test_Case_" + str(test_case_number), HADOOP_OUTPUT_PATH)
    download_file(HADOOP_OUTPUT_PATH, team_folder_path, test_case_number)
    return os.path.join(team_folder_path, test_case_number, "part-00000")
        
        
            
            
            
            

             
            
def test():
    obj_2 = TestOutput()
    output_paths = java_execute_test_cases(os.path.join(CODE_BASE_PATH, 'Code_' + str(1), "WordCount.java"),'1')
    print(output_paths)
    correctness = obj_2.test(output_paths)
    '''output_paths = obj_1.python_execute_test_cases("/home/hduser/Desktop/WordCountF/Code_1_Python/mapper.py",
                                                   "/home/hduser/Desktop/WordCountF/Code_1_Python/reducer.py",'1')
    print(output_paths)
    correctness = obj_2.test(output_paths)'''

test()
