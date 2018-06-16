package storm.twitter.datadriver;

public class OneSecondTuples {
	
	/**
	 * the array of tuples (string)
	 */
	private final String[] tuples;
	
	/**
	 * the number of tuples in this second.
	 * The array is allocated before knowing the exact amount of tuples, so an 
	 * upper bound dimension is used. This is why this field is required and
	 * tuples.length is not meaningful.
	 */
	private final int size;
	
	/**
	 * which second are we talking about?
	 */
	private final int second;
	
	public OneSecondTuples(int second, String[] tuples, int size) {
		this.second = second;
		this.tuples = tuples;
		this.size = size;
	}
	
	public int getSecond() {
		return second;
	}
	
	public String[] getTuples() {
		return tuples;
	}
	
	public int getSize() {
		return size;
	}
}
