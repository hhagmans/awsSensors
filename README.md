# awsSensors
Forked from: https://github.com/awslabs/amazon-kinesis-producer/tree/master/java/amazon-kinesis-producer-sample

Dependencies:
Maven (3.3.9 or newer, older may also work)

Run Producer:
MAVEN_OPTS="-Daws.accessKeyId=YOUR_ACCESS_KEY_ID -Daws.secretKey=YOUR_SECRET_KEY -Dstream.name=STREAM_NAME -Dsensor.name=SENSOR_NAME -Drun.seconds=SECONDS_TO_RUN -Drecord.second=RECORDS_PER_SECOND“ mvn compile -Pproducer exec:java

Run Consumer:
MAVEN_OPTS="-Daws.accessKeyId=YOUR_ACCESS_KEY_ID -Daws.secretKey=YOUR_SECRET_KEY -Dstream.name=STREAM_NAME -Ddb.name=DB_NAME“ mvn compile -Pconsumer exec:java

Run Servlet:
MAVEN_OPTS=„-Daws.accessKeyId=YOUR_ACCESS_KEY_ID -Daws.secretKey=YOUR_SECRET_KEY -Dstream.name=STREAM_NAME -Ddb.name=DB_NAME" mvn compile -PrunServlet exec:java

Delete resources in AWS account:
MAVEN_OPTS="-Daws.accessKeyId=YOUR_ACCESS_KEY_ID -Daws.secretKey=YOUR_SECRET_KEY -Dstream.name=STREAM_NAME -Ddb.name=DB_NAME" mvn compile -PdeleteResources exec:java