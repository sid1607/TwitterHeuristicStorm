package stormsample;

import java.util.Map;
import java.util.Random;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class WordCountSpout extends BaseRichSpout {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	String[] words = {"Hello friends how are you","This is NITK Surathkal","This is a sample Code for Twitter Project","I have become Comfortably Numb","How I wish you were here"};
	SpoutOutputCollector outputcollector;
	Random r;
	@Override
	public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context,SpoutOutputCollector collector) {
		// TODO Auto-generated method stub
		outputcollector=collector;
		r = new Random();
	}

	@Override
	public void nextTuple() {
		// TODO Auto-generated method stub
		outputcollector.emit(new Values(words[r.nextInt(words.length)]));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("words"));
	}
	

}
