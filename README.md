### The Ways of Spring Batch
---
This is the repository for the Ways of Spring Batch talk given at DevNexus 2014.  In this repository, you'll find two things:

1. The presentation as given: [http://mminella.github.io/WaysOfSpringBatch](http://mminella.github.io/WaysOfSpringBatch)
2. The code for the example coded during the talk.

#### Running the code
The code in the example for this talk provides a simple Spring Batch job that divides an Apache log file into multiple files for parallel processing, imports the records into a database geocoding the IP address in each record as it goes, and finally generating an HTML map to display who was watching the Star Wars Kid video.

Before running this code, you'll need to satisfy a couple dependencies:

* Star Wars Kid logs - You can find the logs used for this example here (the link to the torrent itself is at the end of the article): [http://waxy.org/2008/05/star_wars_kid_the_data_dump/](http://waxy.org/2008/05/star_wars_kid_the_data_dump/) 
* Max Mind - This is the library used to geolocate the IP addresses within the log file.  You want the country database which can be found here: [http://dev.maxmind.com/geoip/geoip2/geolite2/](http://dev.maxmind.com/geoip/geoip2/geolite2/)

There are a few hard coded locations within this demo code that you may want to change before running it:

**In BatchConfiguration**

* The location of where the split command will be executed from: `WORKING_DIRECTORY = "/tmp/logs_temp";`
* The location of the MaxMind db file: `MAX_MIND_DB = "/usr/local/share/GeoIP/GeoLite2-Country.mmdb";`
* The location the final report will be generated to: `REPORT_OUTPUT_DIR = "/tmp/logs_temp/output/";`

**In Application**

* The location of the log file to be processed: `args[0] = "inputFile=/tmp/logs_temp/swk_small.log";`
* The location of the where the smaller files (the results of step 1 in the job) will be placed: `args[1] = "stagingDirectory=/tmp/logs_temp/out/";`

Once you have addressed the above for your environment, you should be able to execute the code from STS by right clicking within Application.java and selecting Run As &rarr; Spring Boot App.

#### Configuring an external database
The code as it appears in this repository is configured to use HSQLDB in memory as the database.  If you want to use an external db, you'll need to do three things:

1. Update the application.properties file in src/main/resources with the apropriate values.
2. Update the pom.xml to add the appropriate driver.
3. Create a schema-<MYDATABSE>.sql file in src/main/resources.  This repository provides this file for HSQLDB and MySql.

### Additional features
This repository also contains code for running this job as a partitioned job.  As it is coded it will process four files in parallel until all the smaller files have been processed.  To use this code:

1. In BatchConfiguration.java, comment out the logEntryItemReader bean definition (the entire method).
2. In BatchConfiguration.java, rename the parameter step2 to starWarsJob to partitionedStep2 (both in the method signature and the call to next).  Once you have done this, the starWarsJob method should look like below:

```
@Bean
public Job starWarsJob(Step step1, Step partitionedStep2, Step step3) throws Exception {
	return jobBuilderFactory.get("starWarsJob")
			.incrementer(new RunIdIncrementer())
			.flow(step1)
			.next(partitionedStep2)
			.next(step3)
			.end()
			.build();
}
```





