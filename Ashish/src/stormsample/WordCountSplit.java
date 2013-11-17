package stormsample;

import java.util.Map;
import java.util.StringTokenizer;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class WordCountSplit implements IRichBolt{

	/**
	 * 
	 */
	OutputCollector coll;
	private static final long serialVersionUID = 1L;

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
		coll=collector;
	}

	@Override
	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		StringTokenizer sentence = new StringTokenizer(input.getString(0)," ");
		while(sentence.hasMoreTokens())
		{
			coll.emit(new Values(sentence.nextToken()));
		}
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("words"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}
}