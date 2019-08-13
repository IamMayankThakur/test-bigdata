import os
from hdfs import InsecureClient
from mapreducecodecompiler import MapReduceCodeCompiler
from jarfilegenerator import JarFileGenerator
from mapreduceexecutor import MapReduceExecutor
from testoutput import TestOutput

class ExecuteCode:
    _base_path = "/home/hduser/Desktop/BigData/Teams"
    def execute(self, assignment_number, path_to_code, team_id, test_case_number):
        if(int(assignment_number) == 1):
            path = self._base_path + "/Assignment-1/"
            try:
                os.mkdir(os.path.join(path,team_id))
            except:
                pass
            os.mkdir(os.path.join(path, team_id, test_case_number))
            #print(os.path.join(path, team_id, "Outputs", test_case_number))
            obj_1 = MapReduceCodeCompiler()
            obj_1.compile_code(path_to_code, os.path.join(path,team_id))
            print("[TEST-COMPONENT-LOG]["+assignment_number+"/"+team_id+"] COMPILATION SUCCESSFUL")
            obj_2 = JarFileGenerator()
            obj_2.generate_jar(os.path.join(path,team_id,"ClassFiles",),
                               os.path.join(path,team_id),
                               "WordCountJ")
            print("[TEST-COMPONENT-LOG]["+assignment_number+"/"+team_id+"] JAR FILE CREATED")
            obj_3 = MapReduceExecutor()
            obj_3.execute(os.path.join(path,team_id,"WordCountJ.jar"),
                          "WordCount", "/Test_Case_"+str(test_case_number), "Output/")
            print("[TEST-COMPONENT-LOG]["+assignment_number+"/"+team_id+"] MAP REDUCE EXECUTION SUCCESSFUL")
            
            client = InsecureClient('http://localhost:50070', user='hduser')
            client.download("Output/",os.path.join(path,team_id,test_case_number))
            client.delete('Output/', recursive=True)
            return os.path.join(path,team_id,test_case_number,"Output","part-r-00000")        
            
            
            
            
            
             
            
def test():
    obj_1 = ExecuteCode()
    obj_2 = TestOutput()
    correctness = []
    for test_case in range(1,3):
        output_path = obj_1.execute('1','/home/hduser/Desktop/WordCountF/Code_'+str(2)+'/WordCount.java','2',str(test_case))
        correctness.append(obj_2.check_test_case(output_path,
                                                 "/home/hduser/Desktop/BigData/Outputs/"+str(test_case)+".txt",
                                                  str(test_case)))
    print(correctness)
        
        

test()
