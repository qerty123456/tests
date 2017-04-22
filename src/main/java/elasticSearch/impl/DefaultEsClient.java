package elasticSearch.impl;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import elasticSearch.api.EsClient;

public class DefaultEsClient implements EsClient {

	@SuppressWarnings("resource")
	@Override
	public Client createEsClient(String clusterName) throws Exception {
		Settings settings = Settings.builder().put("cluster.name", clusterName).build();
		TransportClient client = new PreBuiltTransportClient(settings)
				.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));
		return client;
	}

	@Override
	public String createDocument(Client client, String dbName, String tableName, XContentBuilder builder)
			throws Exception {
		IndexResponse response = client.prepareIndex(dbName, tableName).setSource(builder).get();
		return response.getId();
	}

	@Override
	public List<String> readAllDocuments(Client client) throws Exception {
		SearchResponse response = client.prepareSearch().execute().actionGet();
		SearchHit[] searchHits = response.getHits().getHits();
		List<String> results = new ArrayList<>();
		for (SearchHit hit : searchHits) {
			results.add(hit.getSourceAsString());
		}
		return results;
	}

	@Override
	public List<SearchHit> readDocument(Client client, String field, String value) throws Exception {
		SearchResponse response = client.prepareSearch()
				.setTypes()
				.setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
				.setQuery(QueryBuilders.matchQuery(field, value))
				.setFrom(0)
				.setSize(1000)
				.setExplain(false)
				.execute().actionGet();
		if (response.getHits().totalHits() > 1000) {
			throw new IllegalArgumentException("too many results");
		}
		SearchHit[] searchHits = response.getHits().getHits();
		List<SearchHit> results = new ArrayList<>();
		for (SearchHit hit : searchHits) {
			results.add(hit);
		}
		return results;
	}

	@Override
	public void deleteDocuments(Client client, String dbName, String tableName, String id) 
			throws Exception {
		DeleteResponse response = client.prepareDelete(dbName, tableName, id).execute().actionGet();
	}

	@Override
	public void closeClient(Client client) throws Exception {
		client.close();
	}

}
