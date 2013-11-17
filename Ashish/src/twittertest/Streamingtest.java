package twittertest;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.URISyntaxException;

import net.sf.json.JSON;
import net.sf.json.JSONSerializer;
import twitter4j.HashtagEntity;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterException;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;

public final class Streamingtest {
    /**
* Main entry of this application.
*
* @param args
     * @throws IOException 
*/
	static BufferedWriter textfilewriter;
	static ObjectOutputStream jsonfilewriter;
	static int i=0;
    public static void main(String[] args) throws TwitterException, IOException, URISyntaxException
    {
    	ConfigurationBuilder cb = new ConfigurationBuilder();
		cb.setDebugEnabled(true)
		  .setOAuthConsumerKey("KtiWruV6KfQeJ3uHmT4I2w")
		  .setOAuthConsumerSecret("8m0R4vmVic9vYVRizVcvFOyQPVTuwIiUZcpLDtbNchg")
		  .setOAuthAccessToken("66720028-r8gnJxYFhm0WBWwXoEChMmjhTfEKMiohDTHDpNg11")
		  .setOAuthAccessTokenSecret("RWxdMySOinNVQ0pzdAxaRuk0um2OruyBBv4ihoDr4")
		  .setJSONStoreEnabled(true);
        TwitterStream tweetStream = new TwitterStreamFactory(cb.build()).getInstance();
        File textfile = new File(Streamingtest.class.getResource("hashtagtext").toURI());
        if (!textfile.exists())
        	textfile.createNewFile();
        File jsonfile = new File(Streamingtest.class.getResource("statusfilejson.json").toURI());
        if (!jsonfile.exists())
        	jsonfile.createNewFile();
        textfilewriter = new BufferedWriter(new FileWriter(textfile.getAbsoluteFile(),true));
        jsonfilewriter = new ObjectOutputStream(new FileOutputStream(jsonfile));
        StatusListener listener = new StatusListener() {
        	
            @Override
            public void onStatus(Status status) {
            	HashtagEntity hash[]= status.getHashtagEntities();
            		for (int j=0;j<hash.length;j++)
            		{
            			try {
            					textfilewriter.write(hash[j].getText()+"\n");
            			} catch (IOException e) {
            				// TODO Auto-generated catch block
            				System.out.println("Error in text file write");
            				e.printStackTrace();
            			}
            		}
				JSON jsonobj = JSONSerializer.toJSON(status);
            	try {
    				if(i<100)
    				{
    					jsonfilewriter.writeObject(jsonobj);
    					i++;
    				}
    				else
    				{
    					jsonfilewriter.flush();
    					textfilewriter.flush();
    					jsonfilewriter.close();
    					textfilewriter.close();
    					System.out.println("100 Status retrieved!!\nProgram Terminating!!");
    					System.exit(1);
    				}
    			} catch (IOException e) {
    				// TODO Auto-generated catch block
    				System.out.println("Error in json file write");
    				e.printStackTrace();
    			}
            	
            }
            @Override
            public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
                System.out.println("Got a status deletion notice id:" + statusDeletionNotice.getStatusId());
            }

            @Override
            public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
                System.out.println("Got track limitation notice:" + numberOfLimitedStatuses);
            }

            @Override
            public void onScrubGeo(long userId, long upToStatusId) {
                System.out.println("Got scrub_geo event userId:" + userId + " upToStatusId:" + upToStatusId);
            }

            @Override
            public void onStallWarning(StallWarning warning) {
                System.out.println("Got stall warning:" + warning);
            }

            @Override
            public void onException(Exception ex) {
                ex.printStackTrace();
            }
        };
        tweetStream.addListener(listener);
        tweetStream.sample();
    }
}