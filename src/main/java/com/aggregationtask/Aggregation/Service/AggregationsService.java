package com.aggregationtask.Aggregation.Service;

import java.util.Map;

import org.springframework.stereotype.Service;

@Service
public interface AggregationsService {

	public Map<String, Object> RangeAgg(String json);
	
	public Map<String, Object> Comparefields(String json);
	
	public Map<String, Object> Countgreaterthen(String json);
	
}
