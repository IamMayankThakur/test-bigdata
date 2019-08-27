import os
from hdfs import InsecureClient
from mapreducecodecompiler import JavaMapReduceCodeCompiler
from jarfilegenerator import JavaJarFileGenerator
from mapreduceexecutor import JavaMapReduceExecutor
from testoutput import TestOutput
from config import *

class ExecuteCode:
    def java_execute_test_cases(self, assignment_number, path_to_code, team_name):
        output_paths = []
        print("TEST_CASES = "+str(TEST_CASES))
        for test_case in range(1,TEST_CASES+1):
            output_paths.append(self.execute_java(assignment_number,
                                             path_to_code,
                                             team_name,
                                             str(test_case)))
        print("LENGTH OF OUTPUT PATHS = "+str(len(output_paths)))
        return output_paths
    
    @staticmethod
    def create_team_folder(team_name, assignment_number):
        if(int(assignment_number) == 1):
            path = TEAMS_BASE_PATH + "Assignment-1/"
            try:
                os.mkdir(os.path.join(path,team_name))
            except:
                pass
        return os.path.join(path,team_name)
    
    @staticmethod
    def download_file(hdfs_output_path, path, test_case_number):
        client = InsecureClient('http://'+HADOOP_HOST_NAME+':'+HADOOP_NAMENODE_PORT_NUMBER, user=HADOOP_USER_NAME)
        client.download("Output/",os.path.join(path, test_case_number))
        client.delete('Output/', recursive=True)
    
    def execute_java(self, assignment_number, path_to_code, team_name, test_case_number):
        team_folder_path = self.create_team_folder(team_name, assignment_number)
        os.mkdir(os.path.join(team_folder_path, test_case_number))
        obj_1 = JavaMapReduceCodeCompiler()
        obj_1.compile_code(path_to_code, team_folder_path)
        print("[TEST-COMPONENT-LOG]["+assignment_number+"/"+team_name+"] COMPILATION SUCCESSFUL")
        obj_2 = JavaJarFileGenerator()
        obj_2.generate_jar(os.path.join(team_folder_path,"ClassFiles",),
                           team_folder_path,
                           "WordCountJ")
        print("[TEST-COMPONENT-LOG]["+assignment_number+"/"+team_name+"] JAR FILE CREATED")
        obj_3 = JavaMapReduceExecutor()
        obj_3.execute(os.path.join(team_folder_path,"WordCountJ.jar"),
                      "WordCount", "/Test_Case_"+str(test_case_number), "Output/")
        print("[TEST-COMPONENT-LOG]["+assignment_number+"/"+team_name+"] MAP REDUCE EXECUTION SUCCESSFUL")
        self.download_file("Output/", team_folder_path, test_case_number)
        return os.path.join(team_folder_path,test_case_number,"Output","part-r-00000")      
    def execute_python(self, assignment_number, path_to_mapper, path_to_reducer, team_name, test_case_number):
        self.create_team_folder(team_name, assignment_number)
        obj_1 = PythonMapReduceExector()
        obj_1.execute(path_to_mapper, path_to_reducer_code)
        self.download_file("Output/")
        return os.path.join(path,team_name,test_case_number,"Output","part-r-00000")
        
        
            
            
            
            
            
             
            
def test():
    obj_1 = ExecuteCode()
    obj_2 = TestOutput()
    output_paths = obj_1.execute_test_cases('1',CODE_BASE_PATH+'Code_'+str(1)+'/WordCount.java','1')
    correctness = obj_2.test(output_paths)

test()
