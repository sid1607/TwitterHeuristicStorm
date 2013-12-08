package storm.pkg1;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

public class Stream_twitter{
    public static void main(String[] args) throws InterruptedException{
    	     TopologyBuilder builder=new TopologyBuilder();
    	     builder.setSpout("Stream",new Stream_spout());
    	     builder.setBolt("Count",new Stream_bolt()).fieldsGrouping("Stream",new Fields("Timezones_streamed"));
    	     
    	     Config config=new Config();
    	     config.setDebug(true);
    	     LocalCluster cluster=new LocalCluster();
    	     cluster.submitTopology("Timezone_count",config,builder.createTopology());
    	     
    	     Utils.sleep(120000);
    	     cluster.killTopology("Timezone_count");
    	     cluster.shutdown();
    	     System.exit(1);
    }
}
