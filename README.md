# SparkTesting
Simple spark stuff


To make SparkJdbcTest work, simply install the docker and run following commands


docker pull mysql

docker run --name mysqllll --publish=3308:3306 -e MYSQL_ROOT_PASSWORD=password -d mysql


Then connect to mysql with username "root" and password "passwprd"
run the sql command "CREATE SCHEMA testDB;"
