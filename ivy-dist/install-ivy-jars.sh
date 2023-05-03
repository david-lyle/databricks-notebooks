#!/bin/bash
#Only for the driver
if [ ! -z $DB_IS_DRIVER ] && [ $DB_IS_DRIVER = TRUE ] ; then
  rm -f /databricks/jars/----ws_3_3--mvn--hadoop3--org.apache.ivy--ivy--org.apache.ivy__ivy__2.5.0.jar
  sleep 5
	mkdir -p /dbfs/ivy/conf
	cp -f ivy-2.5.0-dev-20230502180208.jar /databricks/jars/.
fi