package com.elasticsearchDemo.elasticDemo.controller;

import java.io.IOException;
import java.text.DecimalFormat;
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
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.filter.Filters;
import org.elasticsearch.search.aggregations.bucket.filter.FiltersAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.filter.FiltersAggregator;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoGrid.Bucket;
import org.elasticsearch.search.aggregations.bucket.geogrid.ParsedGeoHashGrid;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.range.Range;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.ParsedAvg;
import org.elasticsearch.search.aggregations.metrics.Sum;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class BucketAggregation {
	
	RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(new HttpHost("10.1.2.15", 9200, "http")));
	
	// Histogram Aggregation
	@GetMapping("/histogram")
	public Object histogram() throws IOException {
		SearchSourceBuilder searchSourceBuilder=new SearchSourceBuilder();
		AggregationBuilder aggregation=AggregationBuilders.histogram("agg").field("age").interval(1); // interval 1,2,3...
		searchSourceBuilder.aggregation(aggregation);
		
		SearchRequest searchRequest=new SearchRequest("vipul");
		searchRequest.source(searchSourceBuilder);
		SearchResponse searchResponse=client.search(searchRequest,RequestOptions.DEFAULT);
		
		Histogram histogram=searchResponse.getAggregations().get("agg");
		for(Histogram.Bucket entry:histogram.getBuckets()) {
			  Number key = (Number) entry.getKey();   // Key
			  long docCount = entry.getDocCount();    // Doc count
			  
			  System.out.println("key :"+key);
			  System.out.println("Doc Count :"+docCount);
		}return "ok";    }
	//Date Histogram Aggregation
		@GetMapping("/histogram2")
		public Object dateHistogram() throws IOException {
			SearchSourceBuilder searchSourceBuilder=new SearchSourceBuilder();
			AggregationBuilder aggregation=AggregationBuilders.dateHistogram("date").field("dateAndTime")
					//.calendarInterval(DateHistogramInterval.YEAR);
			        .fixedInterval(DateHistogramInterval.days(10));
			searchSourceBuilder.aggregation(aggregation);
			
			SearchRequest searchRequest=new SearchRequest("vipul");
			searchRequest.source(searchSourceBuilder);
			SearchResponse searchResponse=client.search(searchRequest,RequestOptions.DEFAULT);
			
			Histogram histogram=searchResponse.getAggregations().get("date");
			for(Histogram.Bucket entry:histogram.getBuckets()) {
				System.out.println(entry.getKey());                // Key
				System.out.println(entry.getKeyAsString());       // Key as String
				System.out.println(entry.getDocCount());         // Doc count
			}   return "ok";    }
		//Geo Distance Aggregation
		@GetMapping("/distance")
		public Object geoDistance() throws IOException {
			SearchSourceBuilder searchSourceBuilder=new SearchSourceBuilder();
			AggregationBuilder agregation=AggregationBuilders.geoDistance("agg", new GeoPoint(48.84237171118314,2.33320027692004))
					.field("address.location")
	                .unit(DistanceUnit.KILOMETERS)
	                .addUnboundedTo(3.0)
	                .addRange(3.0, 10.0)
	                .addRange(10.0, 500.0);
			searchSourceBuilder.aggregation(agregation);
			
			SearchRequest searchRequest=new SearchRequest("vipul");
			searchRequest.source(searchSourceBuilder);
			SearchResponse searchResponse=client.search(searchRequest,RequestOptions.DEFAULT);
			
			Range range=searchResponse.getAggregations().get("agg");
			for(Range.Bucket entry:range.getBuckets()) {
				String key = entry.getKeyAsString();    // key as String
			    Number from = (Number) entry.getFrom(); // bucket from value
			    Number to = (Number) entry.getTo();     // bucket to value
			    long docCount = entry.getDocCount();    // Doc count
			    
			    System.out.println("key :"+key);
			    System.out.println("from :"+from);
			    System.out.println("to :"+to);
			    System.out.println("doc count :"+docCount);
			}
			
			return "ok";
		}
	//  fild count 
		@GetMapping("/filer")
		public ResponseEntity<?> filterFunc() throws IOException {
			Map<String, Object> map = new HashMap<String, Object>();
			
			SearchSourceBuilder builderPercent = new SearchSourceBuilder();
			FiltersAggregationBuilder filter = AggregationBuilders.filters("agg",new FiltersAggregator.KeyedFilter("firstName", QueryBuilders.termQuery("firstName", "vipul")));
			builderPercent.aggregation(filter);
			
			SearchRequest requestPercent = new SearchRequest("vipul").source(builderPercent);
			//requestPercent.source(builderPercent);
			SearchResponse searchResponse = client.search(requestPercent, RequestOptions.DEFAULT);

			Filters pf = searchResponse.getAggregations().get("agg");
			for (Filters.Bucket entry : pf.getBuckets()) {//String key = entry.getKeyAsString();// bucket key//long docCount = entry.getDocCount(); // Doc count
				map.put("filter_key", entry.getKeyAsString());
				map.put("doc_count", entry.getDocCount());
				}
			return ResponseEntity.ok(map);
			}
		// subAggregation
		@GetMapping("/terms")
		public ResponseEntity<?> termsAggrega() throws IOException{
			List<Object> list=new ArrayList<>();
		    SearchSourceBuilder searchSourceBuilder=new SearchSourceBuilder();
		    // Ipaddres and dateAndTime
		    AggregationBuilder aggregation=AggregationBuilders.terms("agg").field("IPaddress").subAggregation(AggregationBuilders.sum("sumagg").field("age"))
		    		.subAggregation(AggregationBuilders.avg("avg").field("age"));
		    searchSourceBuilder.aggregation(aggregation);
		   
		    SearchRequest searchRequest=new SearchRequest("vipul");
		    searchRequest.source(searchSourceBuilder);
		    SearchResponse searchResponse=client.search(searchRequest, RequestOptions.DEFAULT);
		    
		    Terms terms=searchResponse.getAggregations().get("agg");
		   for(Terms.Bucket b: terms.getBuckets()) {
			   Sum aggValue = b.getAggregations().get("sumagg");
			   ParsedAvg avg=b.getAggregations().get("avg");
		       DecimalFormat formatter = new DecimalFormat("0.00");
		       
		       Map<String, Object> map = new HashMap<String, Object>();
		       map.put("IP count", b.getDocCount());
		       map.put("IP", b.getKeyAsString());
		       map.put("Age total",formatter.format(aggValue.getValue()));
		       map.put("Avg",formatter.format(avg.getValue()));
		       list.add(map);
		   }return ResponseEntity.ok(list);
		}
		@GetMapping("/terms2")////////////////
		public Object termsAggregation2() throws IOException {
			SearchSourceBuilder searchSourceBuilder=new SearchSourceBuilder();
			AggregationBuilder aggregation=AggregationBuilders.filters("agg", new FiltersAggregator.KeyedFilter("firstName",QueryBuilders.termQuery("firstName", "vipul")))
					.subAggregation(AggregationBuilders.terms("Ip").field("IPaddress"));
			searchSourceBuilder.aggregation(aggregation);
			SearchRequest searchRequest=new SearchRequest("vipul");
			searchRequest.source(searchSourceBuilder);
			SearchResponse searchResponse=client.search(searchRequest,RequestOptions.DEFAULT);
			
			Filters filters=searchResponse.getAggregations().get("agg");
			
			for(Filters.Bucket b:filters.getBuckets()) {
				Terms terms=searchResponse.getAggregations().get("Ip");
				 DecimalFormat formatter = new DecimalFormat("0.00");
				 System.out.println(b.getDocCount());
				 System.out.println(b.getKeyAsString());
				 System.out.println(terms);
				}
			
			return "ok";
		}
		@GetMapping("/terms3")
		public Object textFieldAggregation() throws IOException {
			
			SearchSourceBuilder searchSourceBuilder=new SearchSourceBuilder();
			AggregationBuilder aggregation =
				    AggregationBuilders
				        .filters("agg",
				            new FiltersAggregator.KeyedFilter("fname", QueryBuilders.termQuery("firstName", "vipul")),
				            new FiltersAggregator.KeyedFilter("lname", QueryBuilders.termQuery("lastName", "vagadia")),
				            new FiltersAggregator.KeyedFilter("enabals", QueryBuilders.termQuery("enabals", true)));
			searchSourceBuilder.aggregation(aggregation);
			
			SearchRequest searchRequest=new SearchRequest("vipul");
			searchRequest.source(searchSourceBuilder);
			SearchResponse searchResponse=client.search(searchRequest,RequestOptions.DEFAULT);
			
			Filters agg= searchResponse.getAggregations().get("agg");
			for (Filters.Bucket entry : agg.getBuckets()) {
			    String key = entry.getKeyAsString();            // bucket key
			    long docCount = entry.getDocCount();            // Doc count
			
			    System.out.println("key :"+key);
			    System.out.println("count :"+docCount);}
	      return "ok"; }
		// age range
		@GetMapping("/range")
		public Object ageRange() throws IOException {
			SearchSourceBuilder searchSourceBuilder=new SearchSourceBuilder();
			AggregationBuilder aggregation=AggregationBuilders.range("agg").field("age")
					.addUnboundedTo(7)       // 7in under level  count
			        .addRange(3,40)         // 3 to 40 bitwin count
			        .addUnboundedFrom(7);  //  7to uper leval count
			searchSourceBuilder.aggregation(aggregation);
			
			SearchRequest searchRequest=new SearchRequest("vipul");
			searchRequest.source(searchSourceBuilder);
			SearchResponse searchResponse=client.search(searchRequest,RequestOptions.DEFAULT);
			
			Range agg=searchResponse.getAggregations().get("agg");
			for (Range.Bucket entry : agg.getBuckets()) {
			    String key = entry.getKeyAsString();                // Range as key
			    Number from = (Number) entry.getFrom();            // Bucket from
			    Number to = (Number) entry.getTo();               // Bucket to
			    long docCount = entry.getDocCount();             // Doc count
			    
			    System.out.println("key :"+key);
			    System.out.println("from :"+from);
			    System.out.println("to :"+to);
			    System.out.println("doc Count :"+docCount);
			    System.out.println("----------------------------------");
			}return "ok";     }
		// date range
		@GetMapping("/range2")
		public Object dateRange() throws IOException {
			SearchSourceBuilder searchSourceBuilder=new SearchSourceBuilder();
			AggregationBuilder aggregation=AggregationBuilders.dateRange("date").field("dateAndTime").format("yyyy")
					.addUnboundedTo("2021")      // from -infinity in to 1950 (excluded)
	                .addRange("2021", "2022")   // from 1950 to 1960 (excluded)
	                .addUnboundedFrom("2021"); // from 1960 up to +infinity
			searchSourceBuilder.aggregation(aggregation);
			
			SearchRequest searchRequest=new SearchRequest("vipul");
			searchRequest.source(searchSourceBuilder);
			SearchResponse searchResponse=client.search(searchRequest, RequestOptions.DEFAULT);
			
			Range range=searchResponse.getAggregations().get("date");
			for(Range.Bucket entry:range.getBuckets() ) {
				 System.out.println("key :"+entry.getKeyAsString());       // Date range as key
				 System.out.println("date from :"+ entry.getFrom());      // Date bucket from as a Date
				 System.out.println("date to :"+entry.getTo());          // Date bucket to as a Date
				 System.out.println("doc count :"+ entry.getDocCount());// Doc count
				 System.out.println("----------------------------------");
			}return "ok";   }
		// IP range
		@GetMapping("/range3")
		public Object ipRange() throws IOException {
			SearchSourceBuilder searchSourceBuilder=new SearchSourceBuilder();
			AggregationBuilder aggregation=AggregationBuilders.ipRange("ipAgg").field("IPaddress") 
					.addUnboundedTo("10.10.2.15")                  // from -infinity to 192.168.1.0 (excluded)
	                .addRange("10.10.20.15", "192.168.2.0")       // from 192.168.1.0 to 192.168.2.0 (excluded)
	                .addUnboundedFrom("10.111.21.150");         // from 192.168.2.0 to +infinity
			searchSourceBuilder.aggregation(aggregation);
			
			SearchRequest searchRequest=new SearchRequest("vipul");
			searchRequest.source(searchSourceBuilder);
			SearchResponse searchResponse=client.search(searchRequest,RequestOptions.DEFAULT);
			
			Range range=searchResponse.getAggregations().get("ipAgg");
			for(Range.Bucket entry:range.getBuckets()) {
				
				System.out.println("key :"+entry.getKeyAsString());            // Ip range as key);
				System.out.println("from :"+entry.getFromAsString());          // Ip bucket from as a String);
				System.out.println("to :"+entry.getToAsString());           // Ip bucket to as a String);
				System.out.println("doc count :"+entry.getDocCount());            // Doc count
	            System.out.println("----------------------------------");
			}return "ok";     }

		//Geo Hash Grid Aggregation
		@GetMapping("/geohash")
		public Object geoHash() throws IOException {
			SearchSourceBuilder searchSourceBuilder=new SearchSourceBuilder();
			AggregationBuilder aggregation=AggregationBuilders.geohashGrid("agg").field("location").precision(12);
			searchSourceBuilder.aggregation(aggregation);
			
			SearchRequest searchRequest=new SearchRequest("rudra");
			searchRequest.source(searchSourceBuilder);
			SearchResponse searchResponse=client.search(searchRequest,RequestOptions.DEFAULT);
			
			ParsedGeoHashGrid agg = searchResponse.getAggregations().get("agg");


			for (Bucket entry : agg.getBuckets()) {
		    String keyAsString = entry.getKeyAsString(); // key as String
		    GeoPoint key = (GeoPoint) entry.getKey();    // key as geo point
		    long docCount = entry.getDocCount();         // Doc count
			
		    System.out.println("key :["+keyAsString+"]  point :["+key+"]   doc_count :["+docCount+"]");
		   
		}
			return "ok";
		}
		
}
