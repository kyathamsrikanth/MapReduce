# MapReduce
## Homework 1

### Srikanth Kyatham [UIN - 663943205]


### Environment:
**OS** : Mac OS

### PreRequisites:
- SBT 
- Hadoop Version 3
- Java 11

### Referance Project :
LogFileGenerator : https://github.com/0x1DOCD00D/CS441_Fall2022/blob/main/Homeworks/Homework1.md

### Steps to run the project:
1. Clone the repository
2. Please use following compile the project and build assembly jar
   ```sbt clean compile assembly```
3. Check if  far file will be generated in the target folder
4. Start Hadoop by using ```sbin/start-all.sh```
4. Copy the  jar file  and Input into  Hadoop File System.Below is the Copy Command
   ```hadoop fs -put <FileLocalPath> <HDFSPath>```
5. Using below Command to run the MapReduce Job
   ```hadoop jar <HDFSJARPath> <HDFSINPUTPATH> <HDFSOUTPUTPATH> <TaskNumber>```
6. Check  Outputs ```hadoop fs -cat <HDFSoutputFilePath>```



## Classes
1. [TaskOneMapReduce.scala](src/main/scala/TaskOne/TaskOneMapReduce.scala)
2. [TaskTwoMapReduce.scala](src/main/scala/TaskTwo/TaskTwoMapReduce.scala)
3. [TaskThreeMapReduce.scala](src/main/scala/TaskThree/TaskThreeMapReduce.scala)
4. [TaskFourMapReduce.scala](src/main/scala/TaskFour/TaskFourMapReduce.scala)

### Parameters
1. Sample Input file - [LogFileGenerator.2022-10-07.log](src/main/resources/input/LogFileGenerator.2022-10-07.log)
2. Sample Output Path - ```src/main/resources/output```
3. Task Number - (1,2,3,4)


