build:
	javac -classpath lib/zookeeper-3.3.2.jar:lib/log4j-1.2.15.jar MD5Test.java PasswordTask.java ZkConnector.java FileServer.java Worker.java
fss:
	java -classpath lib/zookeeper-3.3.2.jar:lib/log4j-1.2.15.jar:. FileServer
worker:
	java -classpath lib/zookeeper-3.3.2.jar:lib/log4j-1.2.15.jar:. Worker
	
clean:
	rm -f *.class
