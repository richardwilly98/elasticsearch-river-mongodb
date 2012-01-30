package test.elasticsearch.plugin.river.mongodb;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;

import java.io.IOException;
import java.util.Date;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.action.index.IndexRequestBuilder;
import org.elasticsearch.client.action.search.SearchRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class SimpleIndexingSearchingTest {

	private final ESLogger logger = Loggers.getLogger(getClass());

	Client client;
	String INDEX_NAME = "testindex";
	String INDEX_TYPE = "tweet";

	@BeforeClass
	public void beforeClass() {
		logger.info("Initiate client.");
		// String cluster;
		Settings s = ImmutableSettings.settingsBuilder()
		// .put("cluster.name", cluster)
				.build();
		client = new TransportClient(s);
		((TransportClient) client)
				.addTransportAddress(new InetSocketTransportAddress(
						"localhost", 9300));

		try {
			logger.info("Creating index [{}]", INDEX_NAME);
			client.admin().indices().create(new CreateIndexRequest(INDEX_NAME))
					.actionGet();
		} catch (Exception ex) {
			logger.warn("already exists", ex);
		}

	}

	@AfterClass
	public void afterClass() {
		logger.info("Deleting index [{}]", INDEX_NAME);
		client.admin().indices().delete(new DeleteIndexRequest(INDEX_NAME))
				.actionGet();
	}

	@Test
	public void importAttachment() {
		String id = "1";
		try {
			XContentBuilder b = jsonBuilder().startObject();
			b.field("tweetText", "This is a dummy tweet.");
			b.field("fromUserId", "123");
			b.field("createdAt", new Date());
			b.field("userName", "richard");
			b.endObject();

			IndexRequestBuilder irb = client.prepareIndex(INDEX_NAME,
					INDEX_TYPE, id).setSource(b);
			irb.execute().actionGet();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		client.admin().indices().refresh(new RefreshRequest(INDEX_NAME))
				.actionGet();

		GetResponse response = client
				.prepareGet(INDEX_NAME, INDEX_TYPE, String.valueOf(id))
				.execute().actionGet();
		boolean exist = response.exists();
		logger.debug("get exist? " + exist);

		Assert.assertTrue(exist);

		QueryBuilder qb1 = termQuery("userName", "richard");

		SearchRequestBuilder srb = client.prepareSearch(INDEX_NAME).setQuery(
				qb1);

		SearchResponse searchResponse = srb.execute().actionGet();

		logger.info(searchResponse.toString());

		Assert.assertTrue(searchResponse.getHits().getTotalHits() == 1);

		QueryStringQueryBuilder qb = QueryBuilders.queryString("richard");

		logger.info(qb.toString());

		srb = client.prepareSearch(INDEX_NAME).setQuery(qb);

		searchResponse = srb.execute().actionGet();

		logger.info(searchResponse.toString());

		Assert.assertTrue(searchResponse.getHits().getTotalHits() == 1);

		// SearchRequestBuilder builder = client.prepareSearch(INDEX_NAME);
		// QueryStringQueryBuilder qb =
		// QueryBuilders.queryString(queryString).defaultOperator(Operator.AND).
		// field("tweetText").field("userName", 0).
		// allowLeadingWildcard(false).useDisMax(true);
		// builder.addSort("createdAt", SortOrder.DESC);
		// builder.setFrom(page * hitsPerPage).setSize(hitsPerPage);
		// builder.setQuery(qb);
		//
		// SearchResponse rsp = builder.execute().actionGet();
		// SearchHit[] docs = rsp.getHits().getHits();
		// for (SearchHit sd : docs) {
		// //to get explanation you'll need to enable this when querying:
		// //System.out.println(sd.getExplanation().toString());
		//
		// // if we use in mapping: "_source" : {"enabled" : false}
		// // we need to include all necessary fields in query and then to use
		// doc.getFields()
		// // instead of doc.getSource()
		// MyTweet tw = readDoc(sd.getSource(), sd.getId());
		// tweets.add(tw);
		// }

		// // create a query to get this document
		// MatchAllQueryBuilder qb = QueryBuilders.matchAllQuery();
		// TermFilterBuilder fb = FilterBuilders.termFilter("longval", 124L);
		// SearchRequestBuilder srb = client.prepareSearch(INDEX_NAME).
		// setQuery(QueryBuilders.filteredQuery(qb, fb));
		//
		// SearchResponse response = srb.execute().actionGet();
		//
		// logger.info("failed shards:" + response.getFailedShards());
		// Object num = response.getHits().hits()[0].getSource().get("longval");
		// logger.info("longval:" + num);
	}

}
