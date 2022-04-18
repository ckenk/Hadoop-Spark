A MapReduce using Hadoop on Hortonworks Data Platform.

`u.data` file contains the IMDB movie ratings in the form of 
`movie_id`\t`user_id`\t`rating_level`\t`timestamp`

The Java version is created based on `RatingsBreakdown.py`. It can be run in Hortonworks Data Platform VM (HDP)
<pre>python RatingsBreakdown.py -r hadoop --hadoop-streaming-jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar u.data</pre>
Or locally
<pre>python RatingsBreakdown.py u.data</pre>

The Java version of the Python script that tally up the total rating count for each rating level.
To run it extract the dependancy jars of `https://download.jar-download.com/cache_jars/org.apache.hadoop/hadoop-client/2.7.3/jar_files.zip` to the same location as `MapReduceJava.jar` in the HDP VM.

Then to run
locally:<br>
<pre>java -jar MapReduceJava.jar ./u.data ./map_reduce_out</pre>
		
As job submitted to hadoop:<br>
upload the `u.data` using the file browser to `maria_dev` (or the default user directory of the HDP setup. Then run the command:<br>
<pre>yarn jar MapReduceJava.jar ./u.data ./map_reduce_out</pre>
	
* The result will be generated in `map_reduce_out/part-r-00000`
	
	
 
 
