class TestOutput:
    def check_test_case(self, path_to_team_output, path_to_correct_output, 
                        test_case_number):
        team_output = open(path_to_team_output,'r').read()
        correct_output = open(path_to_correct_output,'r').read()
        if(team_output == correct_output):
            print("TEST CASE "+str(test_case_number)+" PASSED")
            return 1
        else:
            print("TEST CASE "+str(test_case_number)+" FAILED")
            return 0
            
        
    