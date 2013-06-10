package storm.starter.bolt;

import java.util.Map;

import storm.trident.spout.IBatchSpout;
import storm.trident.spout.ITridentSpout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;

public class CronSpout implements ITridentSpout<Long> {

	@Override
	public storm.trident.spout.ITridentSpout.BatchCoordinator<Long> getCoordinator(
			String txStateId, Map conf, TopologyContext context) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public storm.trident.spout.ITridentSpout.Emitter<Long> getEmitter(
			String txStateId, Map conf, TopologyContext context) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Fields getOutputFields() {
		// TODO Auto-generated method stub
		return null;
	}

	
}
