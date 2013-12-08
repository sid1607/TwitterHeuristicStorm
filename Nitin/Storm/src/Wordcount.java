import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.StringTokenizer;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.ShellBolt;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;


public class Wordcount {

	//spout to create random sentences
	public static class RandomSentenceSpout extends BaseRichSpout{
        SpoutOutputCollector coll;
        //private static final long serialVersionUID = 1L;
        final String[] words = new String[] {"twitter data","is analyses","kajsd askjdh","sadkhk ashsdk","Twitter data"};
        Random rand;
        public void nextTuple() {
			
			
		    
		    final String word = words[rand.nextInt(4)];
		    
		    coll.emit(new Values(word));
		}
        @Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			
			declarer.declare(new Fields("sentence"));
		}
        @Override
		public void open(Map conf, TopologyContext context,
				SpoutOutputCollector collector) {
			
             coll=collector;
             rand=new Random();
		}
		}
	//end of random words
	
	
	
	
	//bolt to split sentences 
	public static class SplitSentence implements IRichBolt {
            OutputCollector coll;
            //private static final long serialVersionUID = 1L;
		    @Override
		    public void declareOutputFields(OutputFieldsDeclarer declarer) {
		      declarer.declare(new Fields("word"));
		    }

		    @Override
		    public Map<String, Object> getComponentConfiguration() {
		      return null;
		    }

			@Override
			public void prepare(Map stormConf, TopologyContext context,
					OutputCollector collector) {
				// TODO Auto-generated method stub
			       	coll=collector;
			}

			@Override
			public void execute(Tuple input) 
			{
				
				
				
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
		  }
	//end of split sentence
	
	
	
	//bolt to split words
	  public static class WordCount extends BaseRichBolt {
		    HashMap<String, Integer> counts = new HashMap<String, Integer>();
             OutputCollector nwcoll; 
            // private static final long serialVersionUID = 1L;
		    public void execute(Tuple tuple) {
		       String word = tuple.getString(0);
		       
		       Integer count = counts.get(word);
		       if (count == null)
		         count = 0;
		       count++;
		       counts.put(word, count);
		       nwcoll.emit(new Values(word, count));
		    }

		    @Override
		    public void declareOutputFields(OutputFieldsDeclarer declarer) {
		      declarer.declare(new Fields("word", "count"));
		    }

			@Override
			public void prepare(Map stormConf, TopologyContext context,
					OutputCollector collector) {
				// TODO Auto-generated method stub
				nwcoll=collector;
			}

			@Override
			public void cleanup() {
			    super.cleanup();
				try {
					FileWriter fw=new FileWriter("C:\\Users\\Nitin\\Desktop\\Output.txt",true);
					BufferedWriter bw=new BufferedWriter(fw);
					//FileWriter fw=new FileWriter("/home/nitin/Desktop/output.txt",true);
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
	//end of split words
	
	
	  
	  
    //main method
	public static void main(String[] args) throws InterruptedException {

		    TopologyBuilder builder = new TopologyBuilder();
		    
		    builder.setSpout("spout", new RandomSentenceSpout(), 5);

		    builder.setBolt("split", new SplitSentence()).shuffleGrouping("spout");
		    builder.setBolt("count", new WordCount()).fieldsGrouping("split", new Fields("word"));

		    Config conf = new Config();
		    conf.setDebug(true);
             LocalCluster cluster = new LocalCluster();
		      cluster.submitTopology("word-count", conf, builder.createTopology());

		      Utils.sleep(10000);
              cluster.killTopology("word-count");
		      cluster.shutdown();
		      System.exit(1); 
		  }
	
	
	
}
