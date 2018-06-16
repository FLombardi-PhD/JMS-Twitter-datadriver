package storm.twitter.datadriver;

import java.io.PrintStream;
import java.util.Properties;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.InitialContext;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.beust.jcommander.JCommander;

public class TwitterDataDriver {

	private final DataDriverParameters ddp;
	private final TweetReader tweetReader;
	
	private OneSecondTweets firstBuffer, secondBuffer;
	private BufferLoadingThread bufferLoadingThread;
	
	private Logger logger = Logger.getLogger(TwitterDataDriver.class);
	
	private PrintStream throughputLog;
	
	public TwitterDataDriver(DataDriverParameters ddp) throws Exception {
		this.ddp = ddp;
		tweetReader = new TweetReader(ddp);
		
		// pre-load second buffer, so that at the first iteration it gets switched with the first
		secondBuffer = tweetReader.nextSecondTweets();
		bufferLoadingThread = new BufferLoadingThread();
		
		if (ddp.logThroughput)
			throughputLog = new PrintStream("twitter-data-driver-throughput-" + System.currentTimeMillis());
	}
	
	public void start() throws Exception {
		
		Properties props = new Properties();
		props.put("java.naming.factory.initial","org.jnp.interfaces.NamingContextFactory");
		props.put("java.naming.provider.url", ddp.jmsServerHost);
		props.put("java.naming.factory.url.pkgs","org.jboss.naming:org.jnp.interfaces");
		InitialContext context = new InitialContext(props);
		logger.info("Context created!");
		
		Queue queue = (Queue)context.lookup("/queue/tweetQueue");
		logger.info("Queue looked up!");
		
		ConnectionFactory cf = (ConnectionFactory)context.lookup("/ConnectionFactory");
		logger.info("Connection Factory looked up!");
		
		Connection connection = cf.createConnection();
		logger.info("Connection created!");
		
		Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
		logger.info("Session created!");
		
		MessageProducer producer = session.createProducer(queue);
		logger.info("MessageProducer created!");
		
		bufferLoadingThread.start();
		logger.info("Let's start!!!");
		boolean lastSecond = true;
		
		long begin = System.currentTimeMillis();
		long tupleCount = 0;
		
		while (!tweetReader.isEof() || lastSecond) {
			long t = System.currentTimeMillis();
			if (tweetReader.isEof())
				lastSecond = false;
			/**
			 *  At each iteration, send all the tuples in one second (in the 
			 *  first buffer) and in the meanwhile (use another thread) load 
			 *  all the tuples in the next second (in the second buffer).
			 *  The first step is switching first and second buffers and start
			 *  the thread.
			 */
			firstBuffer = secondBuffer;
			bufferLoadingThread.doLoad();
			
			// decide sleep time and batching
			int batchLength = 0;
			int sleepTime = 1000;
			if (firstBuffer.getSize() > 0) {
				do {
					batchLength++;
					sleepTime = 1000 / (int)Math.ceil(firstBuffer.getSize() / batchLength); // sleep time in ms
				} while (sleepTime < ddp.minSleepTime);
			}
			logger.info("[Second " + firstBuffer.getSecond() + "] " + firstBuffer.getSize() + " tuples to send, sleep time set to " + sleepTime + " ms with batches of " + batchLength + " tuples");
			// if (sleepTime == 1000) sleepTime = 500;
			
			String tuples[] = firstBuffer.getTweets();
			long last = System.currentTimeMillis();
			for (int i = 0; i < firstBuffer.getSize(); i++) {
				TextMessage message = session.createTextMessage(tuples[i]);
				producer.send(message);
				tupleCount++;
				logger.debug("Sent line: " + tuples[i]);
				if ((i+1) % batchLength == 0 && i < firstBuffer.getSize() - 1) {
					long now = System.currentTimeMillis();
					int timeToSleep = sleepTime - (int)(now - last);
					sleep(timeToSleep);
					last = now;
				}
			}
			
			// compute time remaining to 1 second
			int remainingTime = 1000 - (int)(System.currentTimeMillis() - t);
			sleep(remainingTime);
			logger.info(firstBuffer.getSize() + " tuples sent in " + (System.currentTimeMillis() - t) + " ms, " + tupleCount + " tuples sent so far");
			logThroughput(firstBuffer.getSecond(), firstBuffer.getSize());
		}
		
		logger.info("DataDriver completed: " + tupleCount + " tuples sent in " + (int)Math.round((double)(System.currentTimeMillis() - begin) / 1000) + " seconds");
		
		// cleanup
		connection.close();
		context.close();
		tweetReader.close();
		bufferLoadingThread.end();
		bufferLoadingThread.join();
		if (throughputLog != null)
			throughputLog.close();
	}
	
	private void logThroughput(int second, int tuples) {
		if (throughputLog != null)
			throughputLog.println(second + "," + tuples);
	}

	/**
	 * This thread is in charge of loading the tuples of the next second while the DataDriver is streaming the tuples of the current second
	 * @author Leonardo
	 *
	 */
	private class BufferLoadingThread extends Thread {
		
		private boolean run = true;
		private Object lock = new Object();
		
		/**
		 * notify the thread to end as soon as possible
		 */
		public void end() {
			run = false;
			doLoad();
		}
		
		/**
		 * wake up run() method and load next second tuples
		 */
		public void doLoad() {
			synchronized (lock) {
				lock.notifyAll();
			}
		}
		
		/* (non-Javadoc)
		 * @see java.lang.Thread#run()
		 * 
		 * Loops loading next second tuples until end is notified.
		 * Blocks at the beginning of each iteration waiting for doLoad() method to be invoked.
		 * 
		 */
		public void run() {
			while (run) {
				try {
					synchronized (lock) {
						lock.wait();
					}
					if (!tweetReader.isEof())
						secondBuffer = tweetReader.nextSecondTweets();
				} catch (Throwable e) {
					Logger.getLogger(BufferLoadingThread.class).error("Error while running BufferLoadingThread", e);
				}
			}
		}
	}

	/**
	 * Stop the execution for the given amount of ms.
	 * If such amount is greater than sleepTimeThreshold parameter, then 
	 * Thread.sleep() is used, otherwise busy waiting is employed.
	 * If sleepTimeAccuracyCheck is enabled, real sleep time is compared with 
	 * given sleep time. In case the difference (in %) is greater than 
	 * sleepTimeAccuracyTolerance, then a warn message is logged.
	 * 
	 * @param sleepTime in ms
	 */
	private void sleep(int sleepTime) {
		if (sleepTime <= 0)
			return;
		
		long t = System.currentTimeMillis();
		
		if (sleepTime > ddp.sleepTimeThreshold) {
			try {
				Thread.sleep(sleepTime);
			} catch (InterruptedException e) {
				logger.error("Error while sleeping", e);
			}
		} else {
			long now = System.currentTimeMillis();
			while (System.currentTimeMillis() < now + sleepTime);
		}
		
		if (ddp.sleepTimeAccuracyCheck) {
			long realSleepTime = System.currentTimeMillis() - t;
			int accuracy = 100 * (int)Math.abs(realSleepTime - sleepTime) / sleepTime;
			if (accuracy > ddp.sleepTimeAccuracyTolerance)
				logger.warn(
					"Bad sleep time accuracy (" + ddp.sleepTimeAccuracyTolerance + "%, sleep time threshold " + ddp.sleepTimeThreshold + " ms): " +
					"expected " + sleepTime + " ms, real " + realSleepTime + " ms");
		}
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		
		DataDriverParameters ddp = new DataDriverParameters();
		new JCommander(ddp, args);
		
		PropertyConfigurator.configure(ddp.log4j);
		
		TwitterDataDriver sdd = new TwitterDataDriver(ddp);
		try {
			Thread.sleep(1000);
			sdd.start();
		} catch (Exception e) {
			Logger.getLogger(TwitterDataDriver.class).error("Error during execution", e);
		}
	}
}
