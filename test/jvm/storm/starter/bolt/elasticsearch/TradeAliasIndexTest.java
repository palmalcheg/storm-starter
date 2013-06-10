package storm.starter.bolt.elasticsearch;

import static org.testng.AssertJUnit.assertTrue;
import static org.elasticsearch.client.Requests.*;
import static org.elasticsearch.index.query.FilterBuilders.nestedFilter;
import static org.elasticsearch.index.query.FilterBuilders.*;
import static org.elasticsearch.index.query.QueryBuilders.*;

import java.io.IOException;
import java.util.Map;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.testng.annotations.Test;

import storm.starter.bolt.elasticsearch.utils.AbstractSharedClusterTest;
import storm.starter.bolt.trade.Trade;

import com.google.common.collect.ImmutableMap;

public class TradeAliasIndexTest extends AbstractSharedClusterTest {
	
	@Test
    public void testSearchingFilteringAliasesSingleIndex() throws Exception {
        // delete all indices
        client().admin().indices().prepareDelete().execute().actionGet();
        
		String trade_index = "trade_idx", trade_type="trade";
        
        run(addMapping(prepareCreate(trade_index), trade_type , new Object[][] { 
        	                                                        {"header", "type", "nested" }, 
        	                                                        {"streams", "type", "nested" } 
        	                                                        })
        );
        ensureGreen();

        logger.info("--> adding filtering aliases to index [{}]",trade_index);
        client().admin().indices().prepareAliases().addAlias(trade_index, "alias1").execute().actionGet();
        client().admin().indices().prepareAliases().addAlias(trade_index, "MW_blotter" , andFilter( 
        		                                                                nestedFilter("header", 
							        		                                      boolFilter().must(termFilter("header.origination", "mw"))
							        		                                    ),
							        		                                    nestedFilter("streams", 
									        		                              boolFilter().must(termFilter("streams.stream_id", "fixed"))
									        		                            )
        		)).execute().actionGet();

        logger.info("--> indexing trade against [{}]",trade_index);
        Trade trade1 = new Trade().header("MW", 1)
        		                  .addStream("float", 10000)
        		                  .addStream("fixed", 20000)
        		           .addParty("partyA", "HWWWWWWWW")
        		           .addParty("partyB", "LON")
        		           ;
        Trade trade2 = new Trade().header("ITRAC", 2)
                .addStream("float", 10000)
                .addStream("fixed", 20000)
         .addParty("partyA", "HWWWWWWWW")
         .addParty("partyB", "LON")
         ;
        client().index(indexRequest(trade_index).type(trade_type).id(String.valueOf(trade1.header.id)).source(trade1.json()).refresh(true)).actionGet();
        client().index(indexRequest(trade_index).type(trade_type).id(String.valueOf(trade2.header.id)).source(trade2.json()).refresh(true)).actionGet();
		
		assertTrue(isMappingExist(trade_index, trade_type));
		
        logger.info("--> checking non filtering alias search");
        SearchResponse searchResponse = client().prepareSearch("alias1").setQuery(matchAllQuery()).execute().actionGet();
        for (SearchHit searchHit : searchResponse.getHits()) {
			logger.info("Score {} Hit trade : {} " , searchHit.getScore() , searchHit.getSourceAsString());
		}
        assertHits(searchResponse.getHits(), "1","2");
        
        logger.info("--> checking MW filtering alias search");
        searchResponse = client().prepareSearch("MW_blotter").setQuery(matchAllQuery()).execute().actionGet();
        for (SearchHit searchHit : searchResponse.getHits()) {
			logger.info("Score {} Hit trade : {} " , searchHit.getScore(), searchHit.getSourceAsString());
		}
        assertHits(searchResponse.getHits(), "1");
        
//        customScoreQuery(matchAllQuery()).
                       
//        logger.info("--> checking quering alias search for nested object ");
//        searchResponse = run(client().prepareSearch(trade_index).setQuery(nestedQuery("header", termQuery("header.origination", "MW"))));
//        for (SearchHit searchHit : searchResponse.getHits()) {
//			logger.info("Hit trade : {} " , searchHit.getSourceAsString());
//		}
//		assertHits(searchResponse.getHits(), "1");
/*
        searchResponse = client().prepareSearch("tests").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet();
        assertHits(searchResponse.getHits(), "1", "2", "3");

        logger.info("--> checking single filtering alias search with sort");
        searchResponse = client().prepareSearch("tests").setQuery(QueryBuilders.matchAllQuery()).addSort("_uid", SortOrder.ASC).execute().actionGet();
        assertHits(searchResponse.getHits(), "1", "2", "3");

        logger.info("--> checking single filtering alias search with global facets");
        searchResponse = client().prepareSearch("tests").setQuery(QueryBuilders.matchQuery("name", "bar"))
                .addFacet(FacetBuilders.termsFacet("test").field("name").global(true))
                .execute().actionGet();
        assertThat(((TermsFacet) searchResponse.getFacets().facet("test")).getEntries().size(), equalTo(4));

        logger.info("--> checking single filtering alias search with global facets and sort");
        searchResponse = client().prepareSearch("tests").setQuery(QueryBuilders.matchQuery("name", "bar"))
                .addFacet(FacetBuilders.termsFacet("test").field("name").global(true))
                .addSort("_uid", SortOrder.ASC).execute().actionGet();
        assertThat(((TermsFacet) searchResponse.getFacets().facet("test")).getEntries().size(), equalTo(4));

        logger.info("--> checking single filtering alias search with non-global facets");
        searchResponse = client().prepareSearch("tests").setQuery(QueryBuilders.matchQuery("name", "bar"))
                .addFacet(FacetBuilders.termsFacet("test").field("name").global(false))
                .addSort("_uid", SortOrder.ASC).execute().actionGet();
        assertThat(((TermsFacet) searchResponse.getFacets().facet("test")).getEntries().size(), equalTo(2));

        searchResponse = client().prepareSearch("foos", "bars").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet();
        assertHits(searchResponse.getHits(), "1", "2");

        logger.info("--> checking single non-filtering alias search");
        searchResponse = client().prepareSearch("alias1").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet();
        assertHits(searchResponse.getHits(), "1", "2", "3", "4");

        logger.info("--> checking non-filtering alias and filtering alias search");
        searchResponse = client().prepareSearch("alias1", "foos").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet();
        assertHits(searchResponse.getHits(), "1", "2", "3", "4");

        logger.info("--> checking index and filtering alias search");
        searchResponse = client().prepareSearch("test", "foos").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet();
        assertHits(searchResponse.getHits(), "1", "2", "3", "4");*/
    }
	
	/**
	 * Check if a mapping already exists in an index
	 * @param index Index name
	 * @param type Mapping name
	 * @return true if mapping exists
	 * @throws IOException 
	 */
	private boolean isMappingExist(String index, String type) throws IOException {
        IndexMetaData imd = null;
        try {
            ClusterState cs = client().admin().cluster().prepareState().setFilterIndices(index).execute().actionGet().getState();
            imd = cs.getMetaData().index(index);
        } catch (IndexMissingException e) {
            // If there is no index, there is no mapping either
        }

        if (imd == null) return false;

		MappingMetaData mdd = imd.mapping(type);
		
		logger.info("Type metadata : {} ", mdd.source().string());

		if (mdd != null) return true;
		return false;
	}


}
