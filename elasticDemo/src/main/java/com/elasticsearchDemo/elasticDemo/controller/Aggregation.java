package com.elasticsearchDemo.elasticDemo.controller;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.missing.Missing;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.Cardinality;
import org.elasticsearch.search.aggregations.metrics.CardinalityAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.MinAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.ParsedAvg;
import org.elasticsearch.search.aggregations.metrics.ParsedMax;
import org.elasticsearch.search.aggregations.metrics.ParsedMin;
import org.elasticsearch.search.aggregations.metrics.ParsedSum;
import org.elasticsearch.search.aggregations.metrics.Percentile;
import org.elasticsearch.search.aggregations.metrics.Percentiles;
import org.elasticsearch.search.aggregations.metrics.PercentilesAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.SumAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.TopHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class Aggregation {
	
	RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(new HttpHost("10.1.2.15", 9200, "http")));
	
	// ------------------------------------------------- Aggregation ----------------------------------------------------------------
	// aggregation sum avreg min and max
		@PostMapping("/aggregation")
		public ResponseEntity<?> aggregation(@RequestParam String caseValue) {
			Map<String, Object> map = new HashMap<String, Object>();
			if (caseValue.equalsIgnoreCase("sum") || caseValue.equalsIgnoreCase("avg") || caseValue.equalsIgnoreCase("min")
					|| caseValue.equalsIgnoreCase("max")) {
				try {
					switch (caseValue.toLowerCase()) {
					case "sum":
						SearchSourceBuilder builderSum = new SearchSourceBuilder();
						SumAggregationBuilder aggregationSum = AggregationBuilders.sum("ageSum").field("age");
						builderSum.aggregation(aggregationSum);
						
						SearchRequest requestSum = new SearchRequest("vipul");
						requestSum.source(builderSum);
						SearchResponse searchResponseSum = client.search(requestSum, RequestOptions.DEFAULT);
						
						Aggregations aggregationsSum = searchResponseSum.getAggregations();
						ParsedSum psum = aggregationsSum.get("ageSum");
						map.put("sum", psum.getValue());
						
						System.out.println("sum of age aggregation :"+psum.getValue());
						break;

					case "avg":
						SearchSourceBuilder builderAvg = new SearchSourceBuilder();
						AvgAggregationBuilder aggregationAvg = AggregationBuilders.avg("ageAvg").field("age");
						builderAvg.aggregation(aggregationAvg);
						
						SearchRequest requestAvg = new SearchRequest("vipul");
						requestAvg.source(builderAvg);
						SearchResponse searchResponseAvg = client.search(requestAvg, RequestOptions.DEFAULT);
						
						Aggregations aggregationsAvg = searchResponseAvg.getAggregations();
						ParsedAvg pavg = aggregationsAvg.get("ageAvg");
						map.put("avg", pavg.getValue());
						
						System.out.println("avg of age aggregation :"+pavg.getValue());
						break;

					case "min":
						
						SearchSourceBuilder builder = new SearchSourceBuilder();
						MinAggregationBuilder min = AggregationBuilders.min("ageMin").field("age");
						builder.aggregation(min);
						
						SearchRequest request = new SearchRequest("vipul");
						request.source(builder);
						SearchResponse searchResponse = client.search(request, RequestOptions.DEFAULT);
						
						Aggregations aggregation = searchResponse.getAggregations();
						ParsedMin pmin = aggregation.get("ageMin");
						map.put("min", pmin.getValue());
						
						System.out.println("min of age aggregation :"+pmin.getValue());
						break;

					case "max":
						SearchSourceBuilder builderMax = new SearchSourceBuilder();
						MaxAggregationBuilder max = AggregationBuilders.max("aggMax").field("age");
						builderMax.aggregation(max);
						
						SearchRequest requestMax = new SearchRequest("vipul");
						requestMax.source(builderMax);
						SearchResponse searchResponseMax = client.search(requestMax, RequestOptions.DEFAULT);
						
						Aggregations aggregationMax = searchResponseMax.getAggregations();
						ParsedMax pmax = aggregationMax.get("aggMax");
						map.put("max", pmax.getValue());
						System.out.println("max of age aggregation :"+pmax.getValue());
						break;
						

					default:
						System.err.println("Wrong keyword");
						break;
					}
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			} else {
				map.put("error", "Please enter keyword from 'sum','avg','min','max'");
			}
			return ResponseEntity.ok(map);
		}
	// integer value
		@GetMapping("/cradinality")
		public ResponseEntity<?> cradinality() throws IOException {
			
			Map<String, Object> map = new HashMap<String, Object>();
			
			SearchSourceBuilder builderCar = new SearchSourceBuilder();
			CardinalityAggregationBuilder aggregation = AggregationBuilders.cardinality("agg").field("age");
			builderCar.aggregation(aggregation);
			
			SearchRequest requestCar = new SearchRequest("vipul");
			requestCar.source(builderCar);
			SearchResponse searchResponseCar = client.search(requestCar, RequestOptions.DEFAULT);
			
			Aggregations agg = searchResponseCar.getAggregations();
			Cardinality pc = (Cardinality) agg.get("agg");
			map.put("cardinality", pc.getValue());
				
			return ResponseEntity.ok(map);
		}
	// integer value
		@GetMapping("/percentil")
		public ResponseEntity<?> percentile() throws IOException {
			
			Map<String, Object> map = new HashMap<String, Object>();
			List listValue = new ArrayList();
			List listPercentage = new ArrayList();
			
			SearchSourceBuilder builderPercent = new SearchSourceBuilder();
			PercentilesAggregationBuilder aggregationPercent = AggregationBuilders.percentiles("aggPercent").field("age");
			builderPercent.aggregation(aggregationPercent);
			
			SearchRequest requestPercent = new SearchRequest("vipul");
			requestPercent.source(builderPercent);
			SearchResponse searchResponsePercent = client.search(requestPercent, RequestOptions.DEFAULT);
			
			Aggregations aggregationsPercent = searchResponsePercent.getAggregations();
			Aggregations aggPercent = searchResponsePercent.getAggregations();
			Percentiles p = aggPercent.get("aggPercent");
			
			for (Percentile per : p) {
					listValue.add(per.getValue());
					listPercentage.add(per.getPercent());
				
			map.put("value", listValue);
			map.put("percentage", listPercentage);
			}
			return ResponseEntity.ok(map);
		}
		@GetMapping("/topHits")
		public ResponseEntity<?> topHits() throws IOException {
			Map<String, Object> map = new HashMap<String, Object>();
			List listKey = new ArrayList();
			List docs = new ArrayList();
			
			SearchSourceBuilder builderHits = new SearchSourceBuilder();
			AggregationBuilder aggregationHits = AggregationBuilders.terms("aggHits").field("age").subAggregation(AggregationBuilders.topHits("top"));
			builderHits.aggregation(aggregationHits);
			
			SearchRequest requestHits = new SearchRequest("vipul");
			requestHits.source(builderHits);
			SearchResponse searchResponsePercent= client.search(requestHits, RequestOptions.DEFAULT);
			
			Aggregations aggregationsPercent = searchResponsePercent.getAggregations();
			Aggregations aggPercent = searchResponsePercent.getAggregations();
//								    Terms aggHits = searchResponsePercent.getAggregations().get("aggHits");
			Terms terms = aggPercent.get("aggHits");
			
			for (Terms.Bucket entry : terms.getBuckets()) {
					listKey.add(entry.getKey()); 
					docs.add(entry.getDocCount());
					
					List listHits = new ArrayList();
					List listData = new ArrayList();
					TopHits topHits = entry.getAggregations().get("top");
					for (SearchHit hit : topHits.getHits().getHits()) {
						listHits.add(hit.getId());
						listData.add(hit.getSourceAsString());
					}
					map.put("hits_id", listHits);
					map.put("hits_data", listData);
					map.put("bucket_key", listKey);
					map.put("doc_count", docs);
				}
			return ResponseEntity.ok(map);
		}
		// mising field count
		@GetMapping("/mising")
		public Object missing() throws IOException {
			SearchSourceBuilder searchSourceBuilder=new SearchSourceBuilder();
			AggregationBuilder aggregation=AggregationBuilders.missing("agg").field("email.keyword");
			searchSourceBuilder.aggregation(aggregation);
			
			SearchRequest searchReuqest=new SearchRequest("vipul");
			searchReuqest.source(searchSourceBuilder);
			SearchResponse searchResponse=client.search(searchReuqest, RequestOptions.DEFAULT);
			
			Missing missing=searchResponse.getAggregations().get("agg");
			return missing.getDocCount();
		}

}
