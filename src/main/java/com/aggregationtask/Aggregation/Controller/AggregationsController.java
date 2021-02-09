package com.aggregationtask.Aggregation.Controller;

import java.util.HashMap;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.aggregationtask.Aggregation.Service.AggregationsService;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;

import java.util.Set;
import org.json.simple.JSONArray;

import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

@RestController
@RequestMapping("/Aggregation")

public class AggregationsController {
	@Autowired
	AggregationsService aggregationsService;

	@GetMapping("/Task1")
	public ResponseEntity<?> UseAggregation(@RequestBody String json)
	{
		Map< String, Object> map=new HashMap<String, Object>();
		map.put("response",aggregationsService.RangeAgg(json));
		return ResponseEntity.ok(map);
	}
	
	@GetMapping("/Task2")
	public ResponseEntity<?> Compare(@RequestBody String json)
	{
		Map< String, Object> map=new HashMap<String, Object>();
		map.put("response",aggregationsService.Comparefields(json));
		return ResponseEntity.ok(map);
	}
	
	@GetMapping("/Task3")
	public ResponseEntity<?> count(@RequestBody String json)
	{
		Map< String, Object> map=new HashMap<String, Object>();
		map.put("response",aggregationsService.Countgreaterthen(json));
		return ResponseEntity.ok(map);
	}


}
