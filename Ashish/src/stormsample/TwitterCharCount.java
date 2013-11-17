package stormsample;

import java.io.File;
import java.io.FileWriter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import twitter4j.GeoLocation;
import twitter4j.Status;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class TwitterCharCount implements IRichBolt {

	/**
	 * 
	 */
	HashMap<String,TZ> stmap = new HashMap<String,TZ>();

	private static final long serialVersionUID = 1L;
	OutputCollector coll;
	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context,	OutputCollector collector) {
		// TODO Auto-generated method stub
		coll = collector;
	}

	@Override
	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		Status status = (Status) input.getValue(0);
		String tz = status.getUser().getTimeZone();
		TZ zone = stmap.get(tz); 
		if(zone==null)
		{
			TZ newzone = new TZ();
			newzone.avgcharcount = (float) status.getText().length();
			newzone.Statuscounted = 1;
			newzone.id = tz;
			stmap.put(tz, newzone);
		}
		else
		{
			stmap.remove(tz);
			zone.avgcharcount = zone.avgcharcount*(zone.Statuscounted/(zone.Statuscounted+1)) + (status.getText().length()/(zone.Statuscounted+1));
			zone.Statuscounted++;
			stmap.put(tz,zone);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("charcount"));
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		try
		{
			File dumpfile = new File("timezonecount.txt");
			if (dumpfile.exists()==false)
				dumpfile.createNewFile();
			FileWriter fw = new FileWriter(dumpfile,true);
			Iterator<String> it = stmap.keySet().iterator();
			while(it.hasNext())
			{
				TZ temp = stmap.get(it.next());
				fw.write("Zone:"+temp.id+"  Count:"+temp.Statuscounted+" AVG:"+temp.avgcharcount+"\n\n");
			}
			fw.flush();
			fw.close();
		}
		catch(Exception e)
		{ System.out.println("Exception in Clean UP");e.printStackTrace();}
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}
	class TZ{
		String id;
		Float avgcharcount;
		Integer Statuscounted;
	}
}
