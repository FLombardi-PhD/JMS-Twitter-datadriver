package storm.twitter.datadriver;

public class OneSecondTweets {
	
	/**
	 * the array of tweets (string)
	 */
	private final String[] tweets;
	
	/**
	 * the number of tweets in this second.
	 * The array is allocated before knowing the exact amount of tweets, so an 
	 * upper bound dimension is used. This is why this field is required and
	 * tweets.length is not meaningful.
	 */
	private final int size;
	
	/**
	 * which second are we talking about?
	 */
	private final int second;
	
	public OneSecondTweets(int second, String[] tuples, int size) {
		this.second = second;
		this.tweets = tuples;
		this.size = size;
	}
	
	public int getSecond() {
		return second;
	}
	
	public String[] getTweets() {
		return tweets;
	}
	
	public int getSize() {
		return size;
	}
}
