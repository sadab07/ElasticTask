package com.aggregationtask.Aggregation.Connection;



import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.RestClientBuilder.HttpClientConfigCallback;

public class ElasticConnection {

	public static RestHighLevelClient elasticConnect()
	{
		final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
		credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials("elastic", "zeronsec@123"));
		System.out.println("Elastic Connected");
		RestClientBuilder builder = RestClient.builder(new HttpHost("10.1.3.25", 9200))
				.setHttpClientConfigCallback(new HttpClientConfigCallback() {
					public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
						return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);}});
		
		RestHighLevelClient client = new RestHighLevelClient(builder);
		return client;
	}
}
