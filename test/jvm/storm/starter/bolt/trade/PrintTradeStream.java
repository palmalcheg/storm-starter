package storm.starter.bolt.trade;


import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import storm.starter.bolt.PrinterBolt;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

@Test
public class PrintTradeStream {        
	
	LocalCluster cluster = new LocalCluster();
	
	@AfterClass
	public void cleanup () {
		try {
        cluster.shutdown();
        }catch(Exception e){}
	}
    
	@Test
	public void testPrint() {
       TopologyBuilder builder = new TopologyBuilder();
        
       Trade t = new Trade()
		            .header("SW", 111)
		                  .addStream("fixed", 100)
		                  .addStream("float", 100)
		            .addParty("A", "DB")
		            .addParty("B", "BARK");
       
       Trade t1 = new Trade()
			       .header("SW", 7777)
				       .addStream("fixed", 100)
				       .addStream("float", 100)
			       .addParty("A", "DB")
			       .addParty("B", "BARK");
        
        
		builder.setSpout("spout", new FeedSpout(t,t1));
        builder.setBolt("print", new PrinterBolt()).addConfiguration(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 1)
                .shuffleGrouping("spout");
        
        Config conf = new Config();
        
        cluster.submitTopology("test", conf, builder.createTopology());
        
        Utils.sleep(10000);
    }
	
	public void testPrintOutTick(){
		TopologyBuilder builder = new TopologyBuilder();
        
	    builder.setBolt("print", new PrinterBolt()).addConfiguration(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 1);
	    
	    cluster.submitTopology("tick_test", new Config(), builder.createTopology());
	    
	    Utils.sleep(10000);
	        
	}
}
