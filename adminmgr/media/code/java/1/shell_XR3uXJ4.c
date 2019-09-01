// No code template for this lab
// Take a whole line as input from the user
// Parse the input (typically based on spaces)
// Use fork() and exec() system calls to run the input that was parsed
// Find if there is a pipe ( "|" ) character in the input
// In case of a pipe, run the two commands using fork() and exec() system calls. Use the pipe() and dup2() system calls to send data between the two processes.
// Assume maximum command length is 64 characters and only a single pipe in the command.