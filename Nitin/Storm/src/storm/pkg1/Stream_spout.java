package storm.pkg1;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.auth.AccessToken;
import twitter4j.conf.ConfigurationBuilder;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

class Stream_spout extends BaseRichSpout{
        private static final long serialVersionUID = 1L;
        SpoutOutputCollector coll;
        private FileReader fr;
        //private String tz=null;
		private BufferedReader br;
		private AccessToken accessToken;
		private ConfigurationBuilder cb;
		private TwitterStream twitterStream;
		private LinkedBlockingQueue<String> queue;
		@Override
		public void open(Map conf, TopologyContext context,SpoutOutputCollector collector)
		{
			     		coll=collector;
			     		queue=new LinkedBlockingQueue<String>(1000);
					     try 
					     {
					    	 		fr=new FileReader("E:\\Nitin\\IEEE\\accessinfo1.txt");
					    	 		br=new BufferedReader(fr);
					    	 		accessToken=loadAccessToken(Long.parseLong(br.readLine()));
					    	 		cb = new ConfigurationBuilder();
					    	 		cb.setDebugEnabled(true);
					    	 		cb.setOAuthConsumerKey("nToX1kRWPclNG1OY5G49bg");
					    	 		cb.setOAuthConsumerSecret("ImwNPuZTO9FxBylx2FbB3yDSA8NwgDDs2dWDn8WtkQw");
					    	 		cb.setOAuthAccessToken(accessToken.getToken());
					    	 		cb.setOAuthAccessTokenSecret(accessToken.getTokenSecret());
					    	 		twitterStream = new TwitterStreamFactory(cb.build()).getInstance();
					    	 		StatusListener listener = new StatusListener() {

								        public void onStatus(Status status) {
								            try {
								            	    queue.offer(status.getUser().getTimeZone().toString());      
											} catch (Exception e) {
												
												e.printStackTrace();
											}
								        }

								        public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
								            System.out.println("Got a status deletion notice id:" + statusDeletionNotice.getStatusId());
								        }

								        public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
								            System.out.println("Got track limitation notice:" + numberOfLimitedStatuses);
								        }

								        public void onScrubGeo(long userId, long upToStatusId) {
								            System.out.println("Got scrub_geo event userId:" + userId + " upToStatusId:" + upToStatusId);
								        }

								        public void onException(Exception ex) {
								            ex.printStackTrace();
								        }

										@Override
										public void onStallWarning(StallWarning arg0) {
											
											
										}
								    };
								    twitterStream.addListener(listener);
									twitterStream.sample();
						    
						 } 
					     catch (NumberFormatException | IOException e) 
					     {
							e.printStackTrace();
						 }
		}

		@Override
		public void nextTuple() 
		{
			 
			String ret = queue.poll();
             if (ret == null) {
                     Utils.sleep(50);
             } else {
                     coll.emit(new Values(ret));
             }				
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			// TODO Auto-generated method stub
			declarer.declare(new Fields("Timezones_streamed"));
		}
		
		private static AccessToken loadAccessToken(long useId) throws IOException
		{
			FileReader fr=new FileReader("E:\\Nitin\\IEEE\\accessinfo.txt");
			BufferedReader br=new BufferedReader(fr);
			String token = br.readLine();
		    String tokenSecret = br.readLine();
		    br.close();
		    return new AccessToken(token, tokenSecret);
		}

}
