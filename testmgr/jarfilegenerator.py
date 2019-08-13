import os

class JarFileGenerator:
    _base_command = "jar -cvf "
    def generate_jar(self, class_dir_path, destination_jar_path, name_of_jar):
        command = (self._base_command + " "
                  + destination_jar_path + "/"
                  + name_of_jar + ".jar " + "-C "
                  + class_dir_path + " .")
        print(command)
        os.system(command)

'''def test():
    obj = JarFileGenerator()
    obj.generate_jar("/home/hduser/Desktop/WordCountF/ClassFiles", 
                     "/home/hduser/Desktop/WordCountF",
                     "WordCountJ")
test()'''
