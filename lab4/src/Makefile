build:
	javac -classpath lib/zookeeper-3.3.2.jar:lib/log4j-1.2.15.jar SerializerHelper.java MD5Test.java PasswordTask.java TaskNodeData.java JobNodeData.java JPacket.java ZkConnector.java FileServer.java Worker.java JobTracker.java
fs:
	java -classpath lib/zookeeper-3.3.2.jar:lib/log4j-1.2.15.jar:. FileServer
worker:
	java -classpath lib/zookeeper-3.3.2.jar:lib/log4j-1.2.15.jar:. Worker
jt:
	java -classpath lib/zookeeper-3.3.2.jar:lib/log4j-1.2.15.jar:. JobTracker
	
clean:
	rm -f *.class
