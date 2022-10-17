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
