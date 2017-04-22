package elasticSearch.api;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.search.SearchHit;
import org.junit.BeforeClass;
import org.junit.Test;

import elasticSearch.impl.DefaultEsClient;

public class EsClientTest {

	private static EsClient esClient;

	@BeforeClass
	public static void setup() throws Exception {
		esClient = new DefaultEsClient();
	}

	@Test
	public void testCreateClient() throws Exception {
		Client client = esClient.createEsClient("elasticsearch");
		assertNotNull(client);
		esClient.closeClient(client);
	}

	@Test
	public void testInsertDocument() throws Exception {
		Client client = esClient.createEsClient("elasticsearch");
		XContentBuilder builder = XContentFactory.jsonBuilder().startObject().field("field", "value").endObject();
		String id = esClient.createDocument(client, "test_db", "test_table", builder);
		esClient.closeClient(client);
		assertNotNull(id);
	}

	@Test
	public void testGetAllDocuments() throws Exception {
		Client client = esClient.createEsClient("elasticsearch");
		XContentBuilder builder1 = XContentFactory.jsonBuilder()
				.startObject().field("field1", "value1").endObject();
		esClient.createDocument(client, "test_db", "test_table", builder1);
		builder1.close();
		XContentBuilder builder2 = XContentFactory.jsonBuilder()
				.startObject().field("field2", "value2").endObject();
		esClient.createDocument(client, "test_db", "test_table", builder2);
		builder2.close();
		XContentBuilder builder3 = XContentFactory.jsonBuilder()
				.startObject().field("field3", "value3").endObject();
		esClient.createDocument(client, "test_db", "test_table", builder3);
		builder3.close();
		List<String> results = esClient.readAllDocuments(client);
		
		assertTrue(results.contains("{\"field1\":\"value1\"}"));
		assertTrue(results.contains("{\"field2\":\"value2\"}"));
		assertTrue(results.contains("{\"field3\":\"value3\"}"));
	}
	
	@Test
	public void testGetDocuments() throws Exception {
		Client client = esClient.createEsClient("elasticsearch");
		XContentBuilder builder1 = XContentFactory.jsonBuilder()
				.startObject().field("field1", "value1").endObject();
		esClient.createDocument(client, "test_db", "test_table", builder1);
		builder1.close();
		XContentBuilder builder2 = XContentFactory.jsonBuilder()
				.startObject().field("field2", "value2").endObject();
		esClient.createDocument(client, "test_db", "test_table", builder2);
		builder2.close();
		XContentBuilder builder3 = XContentFactory.jsonBuilder()
				.startObject().field("field3", "value3").endObject();
		esClient.createDocument(client, "test_db", "test_table", builder3);
		builder3.close();
		List<SearchHit> results = esClient.readDocument(client, "field1", "value1");
		assertTrue(!results.isEmpty());
		System.out.println(results.size());
	}
	
	@Test
	public void testDeleteDocuments() throws Exception {
		Client client = esClient.createEsClient("elasticsearch");
		XContentBuilder builder1 = XContentFactory.jsonBuilder()
				.startObject().field("field1", "value1").endObject();
		esClient.createDocument(client, "test_db", "test_table", builder1);
		builder1.close();
		List<SearchHit> toDelete = esClient.readDocument(client, "field1", "value1");
		for(SearchHit hit : toDelete) {
			System.out.println(hit.getId());
			esClient.deleteDocuments(client, "test_db", "test_table", hit.getId());
		}
		List<SearchHit> results = esClient.readDocument(client, "field1", "value1");
		for(SearchHit hit : results) {
			System.out.println(hit.getId());
			System.out.println(hit.sourceAsString());
		}
		assertTrue(results.isEmpty());
	}

}
