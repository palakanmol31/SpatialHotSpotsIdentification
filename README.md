# Spatial HotSpots Identification

## how to compile the code

--create a project in eclipse

--create a package named com.dds.phase3

--copy the Hotspots.java in this package and add the dependencies.

--compile and run the code

--export the jar


## How to run the code


Starting hadoop


 cd /usr/local/hadoop/sbin/
./start-all.sh 


## Starting spark


cd /home/hduser/spark-2.1.0-bin-hadoop2.7/sbin
./start-master.sh


## Starting slaves:


./start-slaves.sh
----------------------


## Running the jar file:


The format of the command is below:
./bin/spark-submit [spark properties] --class [submission class] [submission jar] [path to input] [path to output]


Go to /spark/bin folder and run the following command
./spark-submit --class com.dds.phase3.Hotspot  /home/hduser/Downloads/bitsplease_phase3.jar "/home/hduser/dds/yellow_tripdata_2015-01.csv" "/home/hduser/dds/bitsplease_phase3_result.csv"
