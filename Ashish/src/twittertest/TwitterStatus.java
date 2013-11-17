package twittertest;

import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.conf.ConfigurationBuilder;
import java.lang.String;
import java.util.Scanner;

public class TwitterStatus {

	public static void main(String arg[])
	{
		ConfigurationBuilder cb = new ConfigurationBuilder();
		cb.setDebugEnabled(true)
		  .setOAuthConsumerKey("KtiWruV6KfQeJ3uHmT4I2w")
		  .setOAuthConsumerSecret("8m0R4vmVic9vYVRizVcvFOyQPVTuwIiUZcpLDtbNchg")
		  .setOAuthAccessToken("66720028-r8gnJxYFhm0WBWwXoEChMmjhTfEKMiohDTHDpNg11")
		  .setOAuthAccessTokenSecret("RWxdMySOinNVQ0pzdAxaRuk0um2OruyBBv4ihoDr4");
		TwitterFactory tf = new TwitterFactory(cb.build());
		Twitter twitter = tf.getInstance();
		Scanner sc = new Scanner(System.in);
		System.out.println("Enter Your New Status:");
		Status status = null;
		try {
			status = twitter.updateStatus(sc.nextLine());
			System.out.println("Successfully updated the status to [" + status.getText() + "].");
		} catch (TwitterException e) {
			// TODO Auto-generated catch block
			System.out.println("Sorry! Unable to update Status");
			e.printStackTrace();
		}
		finally{
			sc.close();
		}
	}
}
