package storm.starter.bolt.trade;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;

import javax.xml.bind.annotation.XmlRootElement;

import org.json.simple.JSONValue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

@XmlRootElement
public class Trade implements Serializable  {
	
	@XmlRootElement
	public static class Stream implements Serializable {
		public String stream_id;
		public BigDecimal notional;
		
	}
	@XmlRootElement
	public static class Header implements Serializable  {
		public String origination;
		public int id;
	}
	
	@XmlRootElement
	public static class Party implements Serializable  {
		public String party_id;
		public String party_code;
	}
	
	public Header header = new Header();
	
	public ArrayList<Trade.Stream> streams = new ArrayList<Trade.Stream>();
	
	public ArrayList<Trade.Party> parties = new ArrayList<Trade.Party>();
	
	public Trade header(String o, int tradeid){
		this.header.origination = o;
		this.header.id = tradeid;
		return this;
	}
	
	public Trade addStream(String stream_id, int notional){
		Stream s = new Stream();
		s.stream_id = stream_id;
		s.notional = BigDecimal.valueOf(notional);
		this.streams.add(s);		
		return this;
	}
	
	public Trade addParty(String id, String code){
		Party p = new Party();
		p.party_id = id;
		p.party_code = code;
		this.parties.add(p);		
		return this;
	}
	
	public String json(){
		try {
			return new ObjectMapper().writeValueAsString(this);
		} catch (JsonProcessingException e) {
			throw new IllegalStateException(e);
		}
	}
	

}
