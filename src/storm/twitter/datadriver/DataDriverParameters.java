package storm.twitter.datadriver;

import com.beust.jcommander.Parameter;

public class DataDriverParameters {

	@Parameter(names = { "-jsh", "-jms-server-host" }, description = "hostname of the JMS server where tuples have to be sent to (default: 127.0.0.1)")
	public String jmsServerHost = "127.0.0.1";
	
	@Parameter(names = { "-jsp", "-jms-server-port" }, description = "TCP port of the JMS server (default: 5445)")
	public Integer jmsServerPort = 5445;
	
	@Parameter(names = { "-ddf", "-data-driver-filename" }, description = "path of the file containing car data points", required = true)
	public String dataDriverfilename;
	
	@Parameter(names = { "-lt", "-log-throughput" }, arity = 1, description = "flag to whether to log throughput to file second by second (default: false)")
	public boolean logThroughput = false;
	
	@Parameter(names = { "-sts", "-seconds-to-skip" }, description = "number of seconds of dataset to skip (default: 0)")
	public Integer secondsToSkip = 0;
	
	@Parameter(names = { "-mts", "-max-tweet-per-second" }, description = "max tweet per second in the dataset (default: 2000)")
	public Integer maxTweetPerSecond = 2000;
	
	@Parameter(names = { "-trf", "-tweet-replication-factor" }, description = "How many tweets to emit from the spout for each tweet received from the JMS queue? (default: 1)")
	public int tweetReplicationFactor = 1;
	
	@Parameter(names = { "-stt", "-sleep-time-threshold" }, description = "threshold (in ms) for deciding whether to sleep or to wait (default: 50)")
	public Integer sleepTimeThreshold = 50;
	
	@Parameter(names = { "-mst", "-min-sleep-time" }, description = "minimum slee time (in ms) allowed before starting to batch tuples (default: 10)")
	public Integer minSleepTime = 10;
	
	@Parameter(names = { "-stat", "-sleep-time-accuracy-tolerance" }, description = "tolerance (%) in the accuracy of real sleep time wrt to expected sleep time (default: 5)")
	public Integer sleepTimeAccuracyTolerance = 5;
	
	@Parameter(names = { "-stac", "-sleep-time-accuracy-check" }, arity = 1, description = "flag to check or not sleep time accuracy (default: false)")
	public boolean sleepTimeAccuracyCheck = false;
	
	@Parameter(names = { "-l", "-log4j" }, description = "path of the log4j configuration file")
	public String log4j = "log4j.properties";
}
