from mapreducecodecompiler import MapReduceCodeCompiler
from jarfilegenerator import JarFileGenerator

class ExecuteCode:
    _base_path = "/home/hduser/Desktop/Team"
    def execute(assignment_number, path_to_code, team_id):
        if(int(assignment_number) == 1):
            obj_1 = MapReduceCodeCompiler()
            obj_1.compile(path_to_code)
            obj_2 = JarFileGenerator()
            
