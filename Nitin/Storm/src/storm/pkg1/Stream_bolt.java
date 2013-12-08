package storm.pkg1;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

class Stream_bolt extends BaseRichBolt{

	private static final long serialVersionUID = 1L;
	HashMap<String, Integer> counts = new HashMap<String, Integer>();
	OutputCollector nwcoll;
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
	
		nwcoll=collector;
	}

	@Override
	public void execute(Tuple input) {
		   String timezone=input.getString(0);
		   Integer count = counts.get(timezone);
	       if (count == null)
	         count = 0;
	       count++;
	       counts.put(timezone, count);
	       nwcoll.emit(new Values(timezone, count)); 
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
		declarer.declare(new Fields("Timezone_streamed","count")); 
	}
	@Override
	public void cleanup()
	{
					super.cleanup();
					try {
						FileWriter fw=new FileWriter("C:\\Users\\Nitin\\Desktop\\Timezones.txt",true);
						BufferedWriter bw=new BufferedWriter(fw);
						for(Map.Entry<String,Integer> entry: counts.entrySet())
						{
						     bw.flush();
							 bw.append(entry.getKey().toString()+"::"+entry.getValue().toString());
						     bw.newLine();
						     bw.flush();
						}
						fw.flush();
						fw.close();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
		
	}

}
