#Start Hadoop
su hduser
start-all.sh

#Test if it is running:
jps	#this should return 6 values on the terminal

#Stop Hadoop
stop-all.sh
____________________________________________________________________________________________________________________________________________

Execute the sample WordCount Program on Hadoop:
The following are the steps to be followed to run the sample WordCount program on Hadoop:
1. Navigate to "http://localhost:50070" on your web browser.
2. Click on the Utilities tab (top right) => Browse the file system
3. The Input directory has already been created
4. Running a java file on Hadoop requires us to follow 3 basic steps:
	i)   Compiling the required java file
	ii)  Creating a .jar file
	iii) Loading the input files onto the localhost
	iv)  Running the java file with the required input files and generating an output file

The following commands must help you go about these steps with ease:

NOTE: ALL COMMANDS WITHIN PARENTHESIS ARE GENERIC COMMANDS

i) Compile
(cd <path to the folder containing the java file>)
cd /home/hduser/Desktop/WordCountF/

javac -classpath /usr/local/hadoop/share/hadoop/common/hadoop-common-2.7.2.jar:/usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-core-2.7.2.jar:/usr/local/hadoop/share/hadoop/common/lib/commons-cli-1.2.jar -d /home/hduser/Desktop/WordCountF *.java

Move manually all the .class files that are generated in /home/hduser/Desktop/WordCountF/ to a new folder, say WordCountC => /home/hduser/Desktop/WordCountF/WordCountC

ii) Create a .jar file
(cd <path to the folder containing the java file>)
cd /home/hduser/Desktop/WordCountF/

jar -cvf WordCountJ.jar -C /home/hduser/Desktop/WordCountF/WordCountC .

iii) Load the input files onto Hadoop
(hadoop fs -put <full path of the file/dir on the local machine> <full path of storage on Hadoop>)
hadoop fs -put /home/hduser/Desktop/WordCountF/inp1.txt /Input/
hadoop fs -put /home/hduser/Desktop/WordCountF/inp2.txt /Input/

iv) Run the .jar file
cd /usr/local/hadoop

v) Go to "hadoop-env.sh" and change HADOOP_CLIENT_OPTS heap size to 2048 MB

(bin/hadoop jar <full path of the jar file created> <Java filename> <full path of the input folder on hadoop> <output folder on hadoop>)

bin/hadoop jar /home/hduser/Desktop/WordCountF/WordCountJ.jar WordCount /Input Output

Navigate to "http://localhost:50070" on your web browser
Click on the Utilities tab (top right) => Browse the file system
Click on user => hduser => Output => part-r-00000 => Download
The downloaded file contains the required output
____________________________________________________________________________________________________________________________________________

Other useful Hadoop commands:

#Deleting a file/dir on Hadoop
hadoop fs -rm -r -skipTrash <full path of the file/dir to be deleted>

#Listing the content of a dir on Hadoop
hadoop fs -ls <path of the dir>

#Help
hadoop fs -help
____________________________________________________________________________________________________________________________________________
