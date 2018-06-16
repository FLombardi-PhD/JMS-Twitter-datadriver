package storm.twitter.datadriver;

import java.io.BufferedReader;
import java.io.FileReader;
import java.text.DecimalFormat;

import org.apache.log4j.Logger;

public class TweetReader {

	private final int maxTweetPerSecond;
	private final int tweetReplicationFactor;
	
	private BufferedReader br;
	
	/**
	 * Checking whether all the tweets in a second have been retrieved requires 
	 * to read a tweet of the next second. Such tweet is still relevant and has 
	 * to be stored somewhere to serve next nextTuple() or nextSecondTuples() 
	 * method.
	 */
	private String bufferLine;
	
	/**
	 * Input dataset may contain no tuples in a certain second, we need to
	 * account for this by returning empty tuple array for seconds when no
	 * tuple is created
	 */
	private int lastSecond;
	
	private int firstSecond;
	
	private boolean eof;
	
	private Logger logger;
	
	public TweetReader(DataDriverParameters ddp) throws Exception {
		logger = Logger.getLogger(TweetReader.class);
		this.maxTweetPerSecond = ddp.maxTweetPerSecond;
		tweetReplicationFactor = ddp.tweetReplicationFactor;
		br = new BufferedReader(new FileReader(ddp.dataDriverfilename));
		logger.info("TweetReader created (max tweet per second: " + maxTweetPerSecond + ", replication factor: " + tweetReplicationFactor + ", seconds to skip: " + ddp.secondsToSkip);
		if (ddp.secondsToSkip > 0)
			skipFirstSeconds(ddp.secondsToSkip);
	}
	
	private void skipFirstSeconds(int howManySeconds) throws Exception {
		logger.info("Skipping first " + howManySeconds + " seconds...");
		long start = System.currentTimeMillis();
		String line = br.readLine();
		int first = getSecond(line);
		long skippedLineCount = 1;
		int last = first;
		long lastLog = start;
		while (last - first < howManySeconds) {
			bufferLine = br.readLine();
			if (bufferLine == null)
				throw new Exception("Trying to skip " + howManySeconds + ", but dataset ended at line " + skippedLineCount);
			last = getSecond(bufferLine);
			skippedLineCount++;
			
			long now = System.currentTimeMillis();
			if (now-lastLog >= 1000) {
				logger.debug("" + skippedLineCount + " lines skipped so far, current second is " + last);
				lastLog = now;
			}
		}
		skippedLineCount--;
		logger.debug("" + skippedLineCount + " skipped lines, first second: " + first + ", last second: " + last);
		logger.debug("Buffer Line: " + bufferLine);
		double time = (double)(System.currentTimeMillis() - start) / 1000;
		this.firstSecond += howManySeconds;
		logger.info("Skipped " + skippedLineCount + " lines, it took " + new DecimalFormat("#.###").format(time) + " seconds (first second becomes " + firstSecond + ")");
	}
	
	/**
	 * @return the next tuple in the file
	 * @throws Exception
	 
	public String nextTuple() throws Exception {
		String tuple;
		if (bufferLine == null) {
			tuple = br.readLine();
			if (tuple == null)
				eof = true;
		} else {
			tuple = bufferLine;
			bufferLine = null;
		}
		return tuple;
	}*/
	
	/**
	 * @return the array of tuples having the same 'second' of the next tuple in the file
	 * @throws Exception
	 */
	public OneSecondTweets nextSecondTweets() throws Exception {
		String tweets[] = new String[maxTweetPerSecond * tweetReplicationFactor];
		int i = 0;
		int theSecond = -1; // the second of these tweets
		String line = null;
		boolean doBreak = false;
		
		// manage a possible tweet read in the previous cycle
		if (bufferLine != null) {
			theSecond = getSecond(bufferLine);
			if (theSecond == lastSecond) {
				for (int j = 0; j < tweetReplicationFactor; j++)
					tweets[i++] = bufferLine;
				bufferLine = null;
			} else {
				doBreak = true; // this tweet is more than one second ahead compared to the tweets read in the previous cycle
				logger.info("Next tweet is at second " + theSecond + ", while this is second " + lastSecond);
			}
		}
		
		while ((line = br.readLine()) != null && !doBreak) {
			int second = getSecond(line);
			if (theSecond == -1) {
				theSecond = second;
				if (theSecond > lastSecond)
					doBreak = true;
			}
			
			if (second == theSecond) {
				for (int j = 0; j < tweetReplicationFactor; j++)
					tweets[i++] = line;
			} else {
				bufferLine = line;
				doBreak = true;
			}
		}
		
		if (line == null)
			eof = true;

		// lastSecond++;
		
		return new OneSecondTweets(lastSecond++, tweets, i);
	}
	
	public boolean isEof() {
		return eof;
	}
	
	/**
	 * @param line
	 * @return the time field (2nd field in the line, in seconds)
	 */
	public int getSecond(String line) {
		int second = (int)(Long.parseLong(line.substring(0, line.indexOf(' ') - 1).trim()) / 1000);
		if (firstSecond == 0)
			firstSecond = second;
		return second - firstSecond;
	}
	
	public void close() throws Exception {
		if (br != null)
			br.close();
	}
}
