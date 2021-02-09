package com.aggregationtask.Aggregation.ServiceImpl;

import java.io.IOException;

import java.util.ArrayList;
import java.util.HashMap;

import java.util.Map;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.metrics.Min;
import org.elasticsearch.search.aggregations.metrics.MinAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.ValueCount;
import org.elasticsearch.search.aggregations.metrics.ValueCountAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.json.JSONObject;
import org.springframework.stereotype.Service;
import org.elasticsearch.search.aggregations.bucket.range.Range;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;

import com.aggregationtask.Aggregation.Connection.ElasticConnection;
import com.aggregationtask.Aggregation.Service.AggregationsService;
import com.fasterxml.jackson.databind.ObjectMapper;

@Service
public class AggregationsServiceImpl implements AggregationsService {

	RestHighLevelClient client = ElasticConnection.elasticConnect();
	ObjectMapper om = new ObjectMapper();
	JSONObject jsonObj;

	@Override
	public Map<String, Object> RangeAgg(String json) {
		Map<String, Object> map = new HashMap<String, Object>();
		jsonObj = new JSONObject(json);

		SearchRequest searchRequest = new SearchRequest((String) jsonObj.get("index"));
		
		String aggName = (String) jsonObj.get("AggregationName");

		
		SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

		//sourceBuilder.aggregation(aggregation);

		searchRequest.source(sourceBuilder);

		SearchResponse searchResponse = null;
		try {
			searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);


		} catch (IOException e) {

			e.printStackTrace();
		}
		ValueCountAggregationBuilder aggregation1 = AggregationBuilders.count((String) jsonObj.get("countName"))
				.field((String) jsonObj.get("CountField"));
		sourceBuilder.aggregation(aggregation1);
		searchRequest.source(sourceBuilder);
		try {
			searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);


		} catch (IOException e) {

			e.printStackTrace();
		}

		ValueCount valuecount = searchResponse.getAggregations().get((String) jsonObj.get("countName"));
		map.put("field", (String) jsonObj.get("CountField"));
		map.put("count : ", valuecount.getValue());
		return map;
	}

	@Override
	public Map<String, Object> Comparefields(String json) {
		
		Map<String, Object> map=new HashMap<String, Object>();
		jsonObj = new JSONObject(json);
		SearchRequest searchRequest = new SearchRequest((String) jsonObj.get("index"));
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		searchSourceBuilder.query(QueryBuilders.matchAllQuery()).fetchSource((String) jsonObj.get("field"), null);
		searchRequest.source(searchSourceBuilder);
	
		SearchResponse searchResponse1 = null;
		try {
			searchResponse1 = client.search(searchRequest,RequestOptions.DEFAULT);
		} catch (IOException e) {
			
			e.printStackTrace();
		}
		ArrayList<Object> list = new ArrayList<>();
		for(SearchHit e : searchResponse1.getHits().getHits())
		{
			list.add(e.getSourceAsString());
		}
		SearchRequest searchRequest1 = new SearchRequest((String) jsonObj.get("index1"));
		SearchSourceBuilder searchSourceBuilder1 = new SearchSourceBuilder();
		searchSourceBuilder1.query(QueryBuilders.matchAllQuery()).fetchSource((String) jsonObj.get("field1"), null);
		searchRequest1.source(searchSourceBuilder);
		
		SearchResponse searchResponse = null;
		try {
			searchResponse = client.search(searchRequest,RequestOptions.DEFAULT);
		} catch (IOException e) {
			
			e.printStackTrace();
		}
		
		ArrayList<Object> list1 = new ArrayList<>();
		for(SearchHit e : searchResponse1.getHits().getHits())
		{
			list1.add(e.getSourceAsString());
		}
		map.put("list",list);
		map.put("list1",list1);
		return map;
	}

	@Override
	public Map<String, Object> Countgreaterthen(String json) {
		Map<String, Object> map = new HashMap<String, Object>();
		jsonObj = new JSONObject(json);

		SearchRequest searchRequest = new SearchRequest((String) jsonObj.get("index"));
		
		ValueCountAggregationBuilder aggregation1 = AggregationBuilders.count("countaggrigation")
				.field((String) jsonObj.get("field"));
	
		SearchSourceBuilder sourceBuilder=new SearchSourceBuilder();
		sourceBuilder.aggregation(aggregation1);
		searchRequest.source(sourceBuilder);

		SearchResponse searchResponse = null;
		try {
			searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);


		} catch (IOException e) {

			e.printStackTrace();
		}
		ValueCount valuecount = searchResponse.getAggregations().get("countaggrigation");
		long count=valuecount.getValue();
		boolean temp=true;
		AggregationBuilder aggregation;
		if (count > 10) {
			aggregation = AggregationBuilders.terms("term").field((String) jsonObj.get("field"));
			
			SearchSourceBuilder sourceBuilder1 = new SearchSourceBuilder();
			
			sourceBuilder1.aggregation(aggregation);
			
			searchRequest.source(sourceBuilder1);
			map.put("count : ", count);
			SearchResponse searchResponse1 = null;
			try {
				searchResponse1 = client.search(searchRequest, RequestOptions.DEFAULT);
			} catch (IOException e) {

				e.printStackTrace();
			}
			Terms agg = searchResponse1.getAggregations().get("term");
			for (Terms.Bucket entry : agg.getBuckets()) {
				Object key = entry.getKey(); // bucket key
				long docCount = entry.getDocCount();
				if(docCount >10)
				{
					temp=false;
					map.put("result : ", temp);
					map.put("count : ", count);
					map.put("Field : ", (String) jsonObj.get("field"));
				}
				else {
					map.put("Result : ", temp);
					map.put("count : ", count);
					map.put("Field : ", (String) jsonObj.get("field"));
				}
				return map;
			}
		}else {
				//map.put("Result : ", temp);
				map.put("count is less than 10 : ", count);
				map.put("Field : ", (String) jsonObj.get("field"));
		}
		
		return map;
	}
}
