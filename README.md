# kuduworkshop
kudu workshop repository

To run any of this you need to set the binDir environment variable so the code knows where kudu binaries are.
This project is only setup to run on OSX 10.11 (El Capitan)

Before you run anything set a global environment variable -DbinDir=$MODULE_DIR$/kudubinary in vm options
You can do this by going to Run > Edit Configurations
Expand the defaults
select Junit and add -DbinDir=$MODULE_DIR$/kudubinary to the end of vmoptions

you'll also need to run brew install openssl

Note that the default logback.xml is set to warning.. you may want to reduce the level to see more.
I have also included lots of other parquet files to play around with joins and stuff in spark if you want to

I would recommend the following path:
SampleCreateAndRead.java 
Then create your own example table and use it for futher testing.
Then run InsertLoadgen to generate fake data

Then switch to SparkExample and run through those and create some of your own.
Then switch to ComplexParallelReadExample
And then review the spark test classes and StartKudu

