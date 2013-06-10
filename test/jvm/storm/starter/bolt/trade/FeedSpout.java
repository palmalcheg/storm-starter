package storm.starter.bolt.trade;

import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class FeedSpout implements IRichSpout {

	private static final long serialVersionUID = 1L;
	Queue<Trade> feedQueue = new LinkedList<Trade>();
	Trade[] feeds;
	private SpoutOutputCollector collector;

	public FeedSpout(Trade... feeds) {
		this.feeds = feeds;
	}

	@Override
	public void nextTuple() {
		Trade nextFeed = feedQueue.poll();
		if(nextFeed != null) {
			Utils.sleep(2000);
			collector.emit(new Values(nextFeed), nextFeed);
		}
	}

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
		for(Trade feed : feeds) {
			feedQueue.add(feed);
		}
	}

	@Override
	public void ack(Object feedId) {
//		feedQueue.add((Trade) feedId);
	}

	@Override
	public void fail(Object feedId) {
//		feedQueue.add((Trade) feedId);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("trade"));
	}

	@Override
	public void close() {
	}

	@Override
	public void activate() {
	}

	@Override
	public void deactivate() {
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
}