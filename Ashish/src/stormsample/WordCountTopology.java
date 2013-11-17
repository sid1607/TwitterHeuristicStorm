package stormsample;

import java.io.File;
import java.io.FileWriter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class WordCountTopology {

	/**
	 * @param args
	 */
	
	public static class ActualCountBolt extends BaseRichBolt{
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		HashMap<String, Integer> hm = new HashMap<String, Integer>();
		OutputCollector oc;
		@Override
		public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector) {
			// TODO Auto-generated method stub
			oc=collector;
		}

		@Override
		public void execute(Tuple input) {
			// TODO Auto-generated method stub
			String word = input.getString(0);
		      Integer count = hm.get(word);
		      if (count == null)
		        count = 0;
		      count++;
		      hm.put(word, count);
		      oc.emit(new Values(word, count));
		    }

		@Override
		public void cleanup() {
			// TODO Auto-generated method stub
			super.cleanup();
			try
			{
				File dumpfile = new File("output1.txt");
				if (dumpfile.exists()==false)
					dumpfile.createNewFile();
				FileWriter fw = new FileWriter(dumpfile,true);
				Iterator<String> it = hm.keySet().iterator();
				while(it.hasNext())
				{
					String key = it.next();
					fw.write(key+"\t"+":"+"\t"+hm.get(key)+"\n");
				}
				fw.flush();
				fw.close();
			}
			catch(Exception e)
			{}
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			// TODO Auto-generated method stub
			declarer.declare(new Fields("words","co"));
		}
	}
	
	public static void main(String[] args) throws InterruptedException {
		// TODO Auto-generated method stub
		TopologyBuilder tb = new TopologyBuilder();
		tb.setSpout("source", new WordCountSpout());
		tb.setBolt("splitsentence", new WordCountSplit()).shuffleGrouping("source");
		tb.setBolt("actualcount", new ActualCountBolt()).fieldsGrouping("splitsentence", new Fields("words"));
		
		Config mycon = new Config();
		mycon.setDebug(true);
		
		LocalCluster myclus = new LocalCluster();
		myclus.submitTopology("Topology", mycon,tb.createTopology());
		
		Utils.sleep(5000);
		myclus.killTopology("Topology");
		myclus.shutdown();
		System.exit(1);
	}
}
