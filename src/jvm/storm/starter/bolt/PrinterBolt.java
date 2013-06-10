package storm.starter.bolt;

import java.util.Date;

import storm.starter.util.TupleHelpers;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;


public class PrinterBolt extends BaseBasicBolt {

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
    	if ( TupleHelpers.isTickTuple(tuple) ){
    		System.out.println("Tick "+tuple);
    	}
    	else {

    		System.out.println(tuple.getSourceStreamId());
    		System.out.println(tuple);
    	}
        
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
    }
    
}
