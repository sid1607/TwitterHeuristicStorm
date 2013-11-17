package stormsample;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

public class TwitterCharCountTopology {
	
	public static void main(String[] args)
	{
		TopologyBuilder tb = new TopologyBuilder();
		tb.setSpout("source", new TwitterStreamSpout());
		tb.setBolt("charcount", new TwitterCharCount()).shuffleGrouping("source");
		
		Config mycon = new Config();
		mycon.setDebug(true);
		
		LocalCluster myclus = new LocalCluster();
		myclus.submitTopology("Topology", mycon,tb.createTopology());
		
		Utils.sleep(100000);
		myclus.killTopology("Topology");
		myclus.shutdown();
		System.exit(1);
	}
}
