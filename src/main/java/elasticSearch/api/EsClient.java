package elasticSearch.api;

import java.util.List;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.SearchHit;

public interface EsClient {
	
	public Client createEsClient(String clusterName) throws Exception;
	
	public String createDocument(Client client, String dbName, String tableName, XContentBuilder builder) throws Exception;
	
	public List<String> readAllDocuments(Client client) throws Exception;
	
	public List<SearchHit> readDocument(Client client, String field, String value) throws Exception;
	
	public void deleteDocuments(Client client, String dbName, String tableName, String id) 
			throws Exception;
	
	public void closeClient(Client client) throws Exception;
}
