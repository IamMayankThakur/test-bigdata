import os
from hdfs import InsecureClient
from mapreducecodecompiler import MapReduceCodeCompiler
from jarfilegenerator import JarFileGenerator
from mapreduceexecutor import MapReduceExecutor
from testoutput import TestOutput
from config import *

class ExecuteCode:
    def execute_test_cases(self, assignment_number, path_to_code, team_name):
        output_paths = []
        print("TEST_CASES = "+str(TEST_CASES))
        for test_case in range(1,TEST_CASES+1):
            output_paths.append(self.execute(assignment_number,
                                             path_to_code,
                                             team_name,
                                             str(test_case)))
        print("LENGTH OF OUTPUT PATHS = "+str(len(output_paths)))
        return output_paths
    def execute(self, assignment_number, path_to_code, team_name, test_case_number):
        if(int(assignment_number) == 1):
            path = TEAMS_BASE_PATH + "Assignment-1/"
            try:
                os.mkdir(os.path.join(path,team_name))
            except:
                pass
            os.mkdir(os.path.join(path, team_name, test_case_number))
            obj_1 = MapReduceCodeCompiler()
            obj_1.compile_code(path_to_code, os.path.join(path, team_name))
            print("[TEST-COMPONENT-LOG]["+assignment_number+"/"+team_name+"] COMPILATION SUCCESSFUL")
            obj_2 = JarFileGenerator()
            obj_2.generate_jar(os.path.join(path,team_name,"ClassFiles",),
                               os.path.join(path,team_name),
                               "WordCountJ")
            print("[TEST-COMPONENT-LOG]["+assignment_number+"/"+team_name+"] JAR FILE CREATED")
            obj_3 = MapReduceExecutor()
            obj_3.execute(os.path.join(path,team_name,"WordCountJ.jar"),
                          "WordCount", "/Test_Case_"+str(test_case_number), "Output/")
            print("[TEST-COMPONENT-LOG]["+assignment_number+"/"+team_name+"] MAP REDUCE EXECUTION SUCCESSFUL")
            
            client = InsecureClient('http://'+HADOOP_HOST_NAME+':'+HADOOP_NAMENODE_PORT_NUMBER, user=HADOOP_USER_NAME)
            client.download("Output/",os.path.join(path,team_name,test_case_number))
            client.delete('Output/', recursive=True)
            return os.path.join(path,team_name,test_case_number,"Output","part-r-00000")        
            
            
            
            
            
             
            
def test():
    obj_1 = ExecuteCode()
    obj_2 = TestOutput()
    output_paths = obj_1.execute_test_cases('1',CODE_BASE_PATH+'Code_'+str(1)+'/WordCount.java','1')
    correctness = obj_2.test(output_paths)

test()
