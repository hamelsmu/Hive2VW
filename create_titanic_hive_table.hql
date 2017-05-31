drop table titanic;

create table titanic (PassengerId STRING, Survived int, Pclass STRING,
Name STRING, Sex STRING, Age int, SibSp STRING,
Parch STRING, Ticket STRING, Fare DOUBLE, Cabin STRING,
Embarked STRING, ds STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\""
)
stored as textfile;

load data local inpath "/home/hamel_husain/titanic.csv" into table titanic;
