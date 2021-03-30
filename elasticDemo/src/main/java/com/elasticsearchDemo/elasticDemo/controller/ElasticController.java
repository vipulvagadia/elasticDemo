package com.elasticsearchDemo.elasticDemo.controller;

import java.io.File;   
import java.io.IOException;
import java.net.InetAddress;
import java.text.DecimalFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.filter.Filters;
import org.elasticsearch.search.aggregations.bucket.filter.FiltersAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.filter.FiltersAggregator;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.missing.Missing;
import org.elasticsearch.search.aggregations.bucket.range.Range;
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
import org.elasticsearch.search.aggregations.metrics.Sum;
import org.elasticsearch.search.aggregations.metrics.SumAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.TopHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.elasticsearchDemo.elasticDemo.service.ElasticsearchService;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

@RestController
public class ElasticController {

	@Autowired
	private ObjectMapper objectMapper;

	@Autowired 
	private ElasticsearchService service;

	RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(new HttpHost("10.1.2.15", 9200, "http")));

	// post data and index creating
	// ---------------------------------------------------------------------------------------------
	// post data
	@PostMapping("/postdata") 
	public Object datSaveElastic() throws IOException {
		
		JsonNode node = new ObjectMapper().readTree(new File("E:/vipul/sts/userDetails/resources/" + File.separator + "dataX.json"));
        for(JsonNode node2:node) {
		IndexRequest indexRequest = new IndexRequest("vipul_vagadia");
		indexRequest.id(node2.get("id").asText());
		indexRequest.source(new ObjectMapper().writeValueAsString(node2), XContentType.JSON);
		IndexResponse indexResponse=client.index(indexRequest, RequestOptions.DEFAULT);
		}
		return node;
	}
	@PostMapping("/postdata2")
	public Object datSaveElastic(@RequestBody JsonNode ob) throws IOException {
		
	    IndexRequest indexRequest = new IndexRequest("vipul");
		indexRequest.id(ob.get("id").asText());
		indexRequest.source(new ObjectMapper().writeValueAsString(ob), XContentType.JSON);
		IndexResponse indexResponse=client.index(indexRequest, RequestOptions.DEFAULT);
		
		return indexResponse;
	}

	// post data 2
	@PostMapping("postdata3")
	public Object dataSaveElastic2() throws IOException {
		
		XContentBuilder builder = XContentFactory.jsonBuilder().startObject().field("firstName", "vipul")
				.field("lastName", "vagadia").field("city", "majevdi").field("stte", "gujarat")
				.field("dateOfBirth", new Date()).field("age", "38").endObject();

		IndexRequest indexRequest = new IndexRequest("rudra");
		indexRequest.source(builder);
        IndexResponse response = client.index(indexRequest, RequestOptions.DEFAULT);
        
		return response;
	} // ----------------------------------------------------------------------------------------------------
		// delete by id
		// delete data by id

	@DeleteMapping("/delete/{id}")
	public String deleteData(@PathVariable String id) throws IOException {
		
		DeleteRequest deleteRequest = new DeleteRequest("vipul", id);
		DeleteResponse deleteResponse = client.delete(deleteRequest, RequestOptions.DEFAULT);
		
		return "delete successfuly";
	}

	// get All data
	// -------------------------------------------------------------------------------------
	// get All
	@GetMapping("/getAllObject")
	public Object postData() throws IOException {
		
		ArrayList<Object> object = new ArrayList<Object>();
		
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		searchSourceBuilder.query(QueryBuilders.matchAllQuery());
		
		SearchRequest searchRequest = new SearchRequest("vipul");
		searchRequest.source(searchSourceBuilder);
		SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
		
			if (searchResponse.getHits().getTotalHits().value > 0) {
				SearchHit[] searchHit = searchResponse.getHits().getHits();
				for (SearchHit hit : searchHit) {
					Map<String, Object> map = hit.getSourceAsMap();
					object.add(objectMapper.convertValue(map, Object.class));
				}}
        return object;
	}

	@GetMapping("/getAllObject2")
	public Object getAllData() throws IOException {
		ArrayList list = new ArrayList();
		
		MatchAllQueryBuilder matchQueryBuilder = QueryBuilders.matchAllQuery();
		SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
		sourceBuilder.query(matchQueryBuilder);
		
		SearchRequest request = new SearchRequest("vipul");
		request.source(sourceBuilder);
        SearchResponse searchResponse = client.search(request, RequestOptions.DEFAULT);

		for (SearchHit s : searchResponse.getHits().getHits()) {
			list.add(s.getSourceAsMap());     }
		
		return list;
	} // -----------------------------------------------------------------------------------------------------search
		// search by id

	@GetMapping("/searchById/{id}")
	public Object searchById(@PathVariable String id) throws IOException {
		List list = new ArrayList();
		
		MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery("id", id);
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		searchSourceBuilder.query(matchQueryBuilder);
		
		SearchRequest searchRequest = new SearchRequest("vipul_vagadia");
		searchRequest.source(searchSourceBuilder);
		SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

		for (SearchHit s : searchResponse.getHits().getHits()) {
			list.add(s.getSourceAsMap());                  }
		return list;
}
    // find by name
	@GetMapping("/findByName/{name}")
	public Object findByNameAll(@PathVariable String name) throws IOException {
		ArrayList<Object> object = new ArrayList<Object>();
		
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		searchSourceBuilder.query(QueryBuilders.boolQuery().must(QueryBuilders.termQuery("firstName", name)));
		// searchSourceBuilder.query(QueryBuilders.boolQuery().must(QueryBuilders.termQuery("firstName.keyword",
		// name)));
		SearchRequest searchRequest = new SearchRequest("vipul_vagadia");
		searchRequest.source(searchSourceBuilder);
		SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
		
			if (searchResponse.getHits().getTotalHits().value > 0) {
				SearchHit[] searchHit = searchResponse.getHits().getHits();
				for (SearchHit hit : searchHit) {
					Map<String, Object> map = hit.getSourceAsMap();
					object.add(objectMapper.convertValue(map, Object.class));
				}   }
        return object;
	}

	@GetMapping("/searchkeyword/{name}")
	public Object findByNameAll2(@PathVariable String name) throws IOException {
		ArrayList<Object> object = new ArrayList<Object>();
		
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		searchSourceBuilder.query(QueryBuilders.boolQuery().must(QueryBuilders.termQuery("firstName.keyword", name)));
		
		SearchRequest searchRequest = new SearchRequest("vipul_vagadia");
		searchRequest.source(searchSourceBuilder);
		SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
		
			if (searchResponse.getHits().getTotalHits().value > 0) {
				SearchHit[] searchHit = searchResponse.getHits().getHits();
				for (SearchHit hit : searchHit) {
					Map<String, Object> map = hit.getSourceAsMap();
					object.add(objectMapper.convertValue(map, Object.class));
				}   }
        return object;
	}

	// first search
	@GetMapping("/search/{name}")
	public Object searchQuery(@PathVariable String name) throws IOException {
		
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery("firstName", name);
		searchSourceBuilder.query(matchQueryBuilder);
		searchSourceBuilder.from(0);
		searchSourceBuilder.size(5);
		
		SearchRequest searchRequest = new SearchRequest("vipul_vagadia");
		searchRequest.source(searchSourceBuilder);
		SearchHits hits = client.search(searchRequest, RequestOptions.DEFAULT).getHits();
		
	    List<Object> collect = Arrays.stream(hits.getHits())
					.map(sourceAsMap -> objectMapper.convertValue(sourceAsMap.getSourceAsMap(), Object.class))
					.collect(Collectors.toList());
	    
			return collect.get(0); // first earch 0 second 1 theerd 2......
		}

	// find by name and city multe match query
	@GetMapping("/findByNameCity/{name}/{city}")
	public Object findByNameAndCity(@PathVariable String name, @PathVariable String city) throws IOException {
		ArrayList<Object> object = new ArrayList<Object>();
		
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		searchSourceBuilder.query(QueryBuilders.boolQuery()
				.must(QueryBuilders.termQuery("firstName", name))
				.must(QueryBuilders.matchQuery("city", city)));
		
		SearchRequest searchRequest = new SearchRequest("vipul_vagadia");
		searchRequest.source(searchSourceBuilder);
		SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
		
			if (searchResponse.getHits().getTotalHits().value > 0) {
				SearchHit[] searchHit = searchResponse.getHits().getHits();
				for (SearchHit hit : searchHit) {
					Map<String, Object> map = hit.getSourceAsMap();
					object.add(objectMapper.convertValue(map, Object.class));
				}
			}
		return object;
	}

	// find by name city and last name multe match query
	@GetMapping("/find/{name}/{city}/{lastName}")
	public Object findByNameCityAndLastName(@PathVariable String name, @PathVariable String city,
			@PathVariable String lastName) throws IOException {
		
		ArrayList<Object> object = new ArrayList<Object>();
		
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		searchSourceBuilder.query(QueryBuilders.boolQuery()
				.must(QueryBuilders.termQuery("firstName", name))
				.must(QueryBuilders.matchQuery("city", city))
				.must(QueryBuilders.matchQuery("lastName", lastName)));
		
		SearchRequest searchRequest = new SearchRequest("vipul_vagadia");
		searchRequest.source(searchSourceBuilder);
		SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
		
			if (searchResponse.getHits().getTotalHits().value > 0) {
				SearchHit[] searchHit = searchResponse.getHits().getHits();
				for (SearchHit hit : searchHit) {
					Map<String, Object> map = hit.getSourceAsMap();
					object.add(objectMapper.convertValue(map, Object.class));
				}}
        return object;
	}

	// search integer tow value in bitvin
	// All index And pecify index search
	@GetMapping("/age")
	public Object findByAgeBitvin() throws IOException {

		SearchSourceBuilder builder = new SearchSourceBuilder()
				.postFilter(QueryBuilders.rangeQuery("age").from(2).to(10));

		SearchRequest searchRequest = new SearchRequest();// searchRequest.indices("vipul_vagadia"); pesify index search
        searchRequest.searchType(SearchType.DFS_QUERY_THEN_FETCH);// All index in search
		searchRequest.source(builder);
		SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);

		return response;
	}// -----------------------------------------------------------------------------------------------------------------
		// delete index
	@DeleteMapping("deletein")
	public AcknowledgedResponse deleteIndex(@RequestParam String index) throws IOException {

		DeleteIndexRequest request = new DeleteIndexRequest(index);
		request.timeout(TimeValue.timeValueMinutes(2));
		request.timeout("2m");
		AcknowledgedResponse delete = client.indices().delete(request, RequestOptions.DEFAULT);
		
		return delete;
	}// ------------------------------------------------------------------------------------------------------------------
		// existing index cheq
		// index is exists true or false
    @GetMapping("/exists")
	public boolean indexexists() {

		// RestHighLevelClient client = getClient();
		GetIndexRequest request = new GetIndexRequest("users");
		request.local(false);
		request.humanReadable(true);
		request.includeDefaults(false);
		try {
			return client.indices().exists(request, RequestOptions.DEFAULT);
		} catch (IOException e) {

			e.printStackTrace();
			return false;
		}
	}// ------------------------------------------------------------------------------------------------------------------update
		// query
		// update mapping
    @PutMapping("/update")
	public String updateData(@RequestBody JsonNode ob) throws IOException {

		IndexRequest request = new IndexRequest("rudra");
		request.id(ob.get("userId").asText());
		request.source(new ObjectMapper().writeValueAsString(ob), XContentType.JSON);
		IndexResponse indexResponse = client.index(request, RequestOptions.DEFAULT);
		
		System.out.println("response id: " + indexResponse.getId());
		return indexResponse.getResult().name();
	}

	@PutMapping("/update/{id}")
	public Object updateFild(@PathVariable String id, @RequestBody Object ob) throws IOException {
		
		ObjectMapper om = new ObjectMapper();
		String jsonString = om.writeValueAsString(ob);
		
		UpdateRequest request = new UpdateRequest("vipul_vagadia", id);
		request.doc(jsonString, XContentType.JSON);
		UpdateResponse updateResponse = client.update(request, RequestOptions.DEFAULT);
		
		return updateResponse;

	}

	// all object field data replace
	@GetMapping("/getField")
	public Object getAllField() throws IOException {

		MatchAllQueryBuilder matchQueryBuilder = QueryBuilders.matchAllQuery();
		SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
		sourceBuilder.query(matchQueryBuilder);
		
		SearchRequest request = new SearchRequest("vipul_vagadia");
		request.source(sourceBuilder);
		SearchResponse searchResponse = client.search(request, RequestOptions.DEFAULT);
		
		ArrayList list = new ArrayList();
		for (SearchHit s : searchResponse.getHits().getHits()) {
			list.add(s.getSourceAsMap());
		}
		
		 InetAddress localhost = InetAddress.getLocalHost();
		 DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");  
		 LocalDateTime now = LocalDateTime.now();  
		 
		JsonNode node = new ObjectMapper().valueToTree(list);
		for (JsonNode n : node) {
			String id = n.get("id").asText();
			
			((ObjectNode) n).put("city", "junagadh");
			((ObjectNode) n).put("email", "vagadiaipul@gmail");
			((ObjectNode) n).put("IPAddress", (localhost.getHostAddress()).trim());
			((ObjectNode) n).put("updateDateAndTime", dtf.format(now));
			
			UpdateRequest request2 = new UpdateRequest("vipul_vagadia", id);
			ObjectMapper om = new ObjectMapper();//String jsonString = om.writeValueAsString(n);//request2.doc(om.writeValueAsString(n), XContentType.JSON);
			client.update(request2.doc(om.writeValueAsString(n), XContentType.JSON), RequestOptions.DEFAULT);
		}
		return list;
	}

	// string split email
	@GetMapping("/getField2")
	public Object getAllField2() throws IOException {
        MatchAllQueryBuilder matchQueryBuilder = QueryBuilders.matchAllQuery();
		SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
		sourceBuilder.query(matchQueryBuilder);
		
		SearchRequest request = new SearchRequest("vipul_vagadia");
		request.source(sourceBuilder);
		SearchResponse searchResponse = client.search(request, RequestOptions.DEFAULT);
		
		ArrayList list = new ArrayList();
		for (SearchHit s : searchResponse.getHits().getHits()) {
			list.add(s.getSourceAsMap());
		}
		
		JsonNode node = new ObjectMapper().valueToTree(list);
		for (JsonNode n : node) {
			
			String id = n.get("id").asText();
			String email = n.get("email").asText();
			String[] ss = email.split("@", 5);
			String p1 = ss[0] + "@gmail.com";
			
			((ObjectNode) n).put("email", p1);
			
			UpdateRequest request2 = new UpdateRequest("vipul_vagadia", id);
			ObjectMapper om = new ObjectMapper();//String jsonString = om.writeValueAsString(n);//request2.doc(om.writeValueAsString(n), XContentType.JSON);
			client.update(request2.doc(om.writeValueAsString(n), XContentType.JSON), RequestOptions.DEFAULT);
		}
		return list;
	}

	// search by name and update field data
	@PostMapping("/name/{name}")
	public Object updateByName(@PathVariable String name, @RequestParam String key, @RequestParam String value) throws IOException {
		ArrayList<Object> list = new ArrayList<Object>();
		
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		searchSourceBuilder.query(QueryBuilders.boolQuery().must(QueryBuilders.termQuery("firstName.keyword", name)));
		
		SearchRequest searchRequest = new SearchRequest("vipul_vagadia");
		searchRequest.source(searchSourceBuilder);
		SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
		
			if (searchResponse.getHits().getTotalHits().value > 0) {
				for (SearchHit hit : searchResponse.getHits().getHits()) {
					list.add(hit.getSourceAsMap());
				}
			}
			JsonNode node = new ObjectMapper().valueToTree(list);
			for (JsonNode n : node) {
				String id = n.get("id").asText();
				((ObjectNode) n).put(key, value); // update field
				
				UpdateRequest request2 = new UpdateRequest("vipul_vagadia", id);
				ObjectMapper om = new ObjectMapper();//String jsonString = om.writeValueAsString(n);//request2.doc(om.writeValueAsString(n), XContentType.JSON);
				client.update(request2.doc(om.writeValueAsString(n), XContentType.JSON), RequestOptions.DEFAULT);
			}
		return list;
	}

	/// index creating
	/// -----------------------------------------------------------------------------------------------------
	/// index creating
	@PostMapping("/file")
	public CreateIndexResponse files(@RequestParam String index)
			throws JsonParseException, JsonMappingException, IOException {
		CreateIndexRequest request = new CreateIndexRequest(index);
		request.settings(Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 2));
		JsonNode node = new ObjectMapper().readValue(new File("E:\\vipul\\files\\indexType.json"), JsonNode.class);
		request.mapping(node.toString(), XContentType.JSON);
		request.alias(new Alias("twitter_alias").filter(QueryBuilders.termQuery("user", "kimchy")));
		CreateIndexResponse createIndexResponse = client.indices().create(request, RequestOptions.DEFAULT);
		return createIndexResponse;
	}

	@GetMapping("/indextype2")
	public CreateIndexResponse indexType() throws IOException {

		CreateIndexRequest request = new CreateIndexRequest("twitter1");
		request.settings(Settings.builder().put("index.number_of_shards", 3).put("index.number_of_replicas", 2));
		XContentBuilder builder = XContentFactory.jsonBuilder();
		builder.startObject();
		{
			builder.startObject("properties");
			{
				builder.startObject("message");
				{
					builder.field("type", "text");
				}
				builder.endObject();
				builder.startObject("name");
				{
					builder.field("type", "text");
				}
				builder.endObject();
				builder.startObject("id");
				{
					builder.field("type", "integer");
				}
				builder.endObject();

			}
			builder.endObject();
		}
		builder.endObject();
		request.mapping(builder);
		request.alias(new Alias("twitter_alias").filter(QueryBuilders.termQuery("user", "kimchy")));
		CreateIndexResponse createIndexResponse = client.indices().create(request, RequestOptions.DEFAULT);
		return createIndexResponse;
	}

	// index criating and key type
	@GetMapping("/indextype")
	public CreateIndexResponse indexpost() throws IOException {

		CreateIndexRequest request = new CreateIndexRequest("rudra4");
		request.settings(Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 2));
		Map<String, Object> message = new HashMap<>();
		message.put("type", "text");
		Map<String, Object> message2 = new HashMap<>();
		message2.put("type", "long");

		Map<String, Object> properties = new HashMap<>();
		properties.put("userId", message2);
		properties.put("name", message);
		properties.put("lastName", message);
		properties.put("city", message);

		Map<String, Object> mapping = new HashMap<>();
		mapping.put("properties", properties);
		System.out.println(mapping);
		request.mapping(mapping);
		CreateIndexResponse indexResponse = client.indices().create(request, RequestOptions.DEFAULT);
		System.out.println("response id: " + indexResponse.index());

		return indexResponse;
	}

	// index criating
	@PostMapping("/index")
	public CreateIndexResponse indexCriating(@RequestParam String indexName) throws IOException {
		try {
			CreateIndexRequest request = new CreateIndexRequest(indexName);
			request.settings(Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 2));
			CreateIndexResponse createIndexResponse = client.indices().create(request, RequestOptions.DEFAULT);

			System.out.println("criating index name: " + createIndexResponse.index());
			return createIndexResponse;
		} catch (Exception e) {
			return null;
		}
	}
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
//							    Terms aggHits = searchResponsePercent.getAggregations().get("aggHits");
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
//  fild count 
	@GetMapping("/filer")
	public ResponseEntity<?> filterFunc() throws IOException {
		Map<String, Object> map = new HashMap<String, Object>();
		
		SearchSourceBuilder builderPercent = new SearchSourceBuilder();
		FiltersAggregationBuilder filter = AggregationBuilders.filters("agg",new FiltersAggregator.KeyedFilter("firstName", QueryBuilders.termQuery("firstName", "vipul")));
		builderPercent.aggregation(filter);
		
		SearchRequest requestPercent = new SearchRequest("vipul");
		requestPercent.source(builderPercent);
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
	//Geo Hash Grid Aggregation
	@GetMapping("/geohash")
	public Object geoHash() throws IOException {
		SearchSourceBuilder searchSourceBuilder=new SearchSourceBuilder();
		AggregationBuilder aggregation=AggregationBuilders.geohashGrid("agg").field("age").precision(1);
		searchSourceBuilder.aggregation(aggregation);
		
		SearchRequest searchRequest=new SearchRequest("vipul");
		searchRequest.source(searchSourceBuilder);
		SearchResponse searchResponse=client.search(searchRequest,RequestOptions.DEFAULT);
		
//		GeoHashGrid agg = searchResponse.getAggregations().get("agg");

//		// For each entry
//		for (GeoHashGrid.Bucket entry : agg.getBuckets()) {
//		    String keyAsString = entry.getKeyAsString(); // key as String
//		    GeoPoint key = (GeoPoint) entry.getKey();    // key as geo point
//		    long docCount = entry.getDocCount();         // Doc count
//
//		   // logger.info("key [{}], point {}, doc_count [{}]", keyAsString, key, docCount);
//		}
		return "ok";
	}
}




/*
System.out.println("IP count :"+b.getDocCount());
System.out.println("Key IP :"+b.getKeyAsString());
System.out.println("Age total :"+formatter.format(aggValue.getValue()));
 System.out.println("-----------------------------------------------");
*/