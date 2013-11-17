package stormsample;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class TwitterStreamSpout extends BaseRichSpout{

	/**
	 * This is the Twitter Stream Spout.
	 * It fetches twitter status objects !!
	 */
	private static final long serialVersionUID = 1L;

	SpoutOutputCollector coll;
	LinkedBlockingQueue<Status> statusqueue = new LinkedBlockingQueue<Status>();
	TwitterStream tStream;
	@Override
	public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context,SpoutOutputCollector collector) {
		// TODO Auto-generated method stub
		coll=collector;
		
		//Configuring Twitter OAuth
		ConfigurationBuilder cb = new ConfigurationBuilder();
		cb.setDebugEnabled(true)
		  .setOAuthConsumerKey("KtiWruV6KfQeJ3uHmT4I2w")
		  .setOAuthConsumerSecret("8m0R4vmVic9vYVRizVcvFOyQPVTuwIiUZcpLDtbNchg")
		  .setOAuthAccessToken("66720028-r8gnJxYFhm0WBWwXoEChMmjhTfEKMiohDTHDpNg11")
		  .setOAuthAccessTokenSecret("RWxdMySOinNVQ0pzdAxaRuk0um2OruyBBv4ihoDr4")
		  .setJSONStoreEnabled(true);
		
		//Twitter Stream Declaration
		tStream = new TwitterStreamFactory(cb.build()).getInstance();
		
		StatusListener listener = new StatusListener(){

			@Override
			public void onStatus(Status arg0) {
				statusqueue.offer(arg0);
			}
			//Irrelevant Functions
			@Override
			public void onException(Exception arg0) {}
			@Override
			public void onDeletionNotice(StatusDeletionNotice arg0) {}
			@Override
			public void onScrubGeo(long arg0, long arg1) {}
			@Override
			public void onStallWarning(StallWarning arg0) {}
			@Override
			public void onTrackLimitationNotice(int arg0) {}
			
		};
		tStream.addListener(listener);
        tStream.sample();
	}

	@Override
	public void nextTuple() {
		// TODO Auto-generated method stub
		Status tempst = statusqueue.poll();
		if(tempst==null)
			Utils.sleep(50);
		else
			coll.emit(new Values(tempst));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("tweet"));
	}

}
