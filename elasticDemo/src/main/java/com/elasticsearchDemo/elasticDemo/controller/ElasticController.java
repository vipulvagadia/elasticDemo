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
		//indexRequest.source(new ObjectMapper().writeValueAsString(node2), XContentType.JSON);
		indexRequest.source(objectMapper.writeValueAsString(node2), XContentType.JSON);
		IndexResponse indexResponse=client.index(indexRequest, RequestOptions.DEFAULT);
		}
		return node;
	}
	@PostMapping("/postdata2")
	public Object datSaveElastic(@RequestBody JsonNode ob) throws IOException {
		
	    IndexRequest indexRequest = new IndexRequest("rudra");
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
		
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(QueryBuilders.matchAllQuery());
		//searchSourceBuilder.query(QueryBuilders.matchAllQuery());
		
		SearchRequest searchRequest = new SearchRequest("rudra").source(searchSourceBuilder);
		//searchRequest.source(searchSourceBuilder);
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
		SearchSourceBuilder sourceBuilder = new SearchSourceBuilder().query(matchQueryBuilder);
		//sourceBuilder.query(matchQueryBuilder);
		
		SearchRequest request = new SearchRequest("vipul").source(sourceBuilder);
		//request.source(sourceBuilder);
        SearchResponse searchResponse = client.search(request, RequestOptions.DEFAULT);

		for (SearchHit s : searchResponse.getHits().getHits()) {
			list.add(s.getSourceAsMap());     }
		
		return list;
	} // -----------------------------------------------------------------------------------------------------search
		// search by id

	@GetMapping("/searchById/{id}")
	public Object searchById(@PathVariable String id) throws IOException {
		List list = new ArrayList();
		
		//MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery("id", id);
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(QueryBuilders.matchQuery("id", id));
		//searchSourceBuilder.query(QueryBuilders.matchQuery("id", id));
		
		SearchRequest searchRequest = new SearchRequest("vipul").source(searchSourceBuilder);
		//searchRequest.source(searchSourceBuilder);
		SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

		for (SearchHit s : searchResponse.getHits().getHits()) {
			list.add(s.getSourceAsMap());                  }
		return list;
}
    // find by name
	@GetMapping("/findByName/{name}")
	public Object findByNameAll(@PathVariable String name) throws IOException {
		ArrayList<Object> object = new ArrayList<Object>();
		
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(QueryBuilders.boolQuery().must(QueryBuilders.termQuery("firstName", name)));
		//searchSourceBuilder.query(QueryBuilders.boolQuery().must(QueryBuilders.termQuery("firstName", name)));
		// searchSourceBuilder.query(QueryBuilders.boolQuery().must(QueryBuilders.termQuery("firstName.keyword",
		// name)));
		SearchRequest searchRequest = new SearchRequest("vipul").source(searchSourceBuilder);
		//searchRequest.source(searchSourceBuilder);
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
		
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(QueryBuilders.boolQuery().must(QueryBuilders.termQuery("firstName.keyword", name)));
		//searchSourceBuilder.query(QueryBuilders.boolQuery().must(QueryBuilders.termQuery("firstName.keyword", name)));
		
		SearchRequest searchRequest = new SearchRequest("vipul").source(searchSourceBuilder);
		//searchRequest.source(searchSourceBuilder);
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
		//MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery("firstName", name);
		searchSourceBuilder.query( QueryBuilders.matchQuery("firstName", name));
		searchSourceBuilder.from(0);
		searchSourceBuilder.size(5);
		
		SearchRequest searchRequest = new SearchRequest("vipul").source(searchSourceBuilder);
		//searchRequest.source(searchSourceBuilder);
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
		
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(QueryBuilders.boolQuery()
				.must(QueryBuilders.termQuery("firstName", name))
				.must(QueryBuilders.matchQuery("city", city)));
		//searchSourceBuilder.query(QueryBuilders.boolQuery()
				//.must(QueryBuilders.termQuery("firstName", name))
				//.must(QueryBuilders.matchQuery("city", city)));
		
		SearchRequest searchRequest = new SearchRequest("vipul_vagadia").source(searchSourceBuilder);
		//searchRequest.source(searchSourceBuilder);
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
		
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(QueryBuilders.boolQuery()
				.must(QueryBuilders.termQuery("firstName", name))
				.must(QueryBuilders.matchQuery("city", city))
				.must(QueryBuilders.matchQuery("lastName", lastName)));
		
		SearchRequest searchRequest = new SearchRequest("vipul_vagadia").source(searchSourceBuilder);
		//searchRequest.source(searchSourceBuilder);
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
	public Object deleteIndex(@RequestParam String index) throws IOException {

		GetIndexRequest grequest = new GetIndexRequest(index);
		boolean b=client.indices().exists(grequest, RequestOptions.DEFAULT);
		if(b == false) {
			return "index is not avelabal";
		}
		
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
	public Object files(@RequestParam String index)
	                         throws JsonParseException, JsonMappingException, IOException {
		
		GetIndexRequest grequest = new GetIndexRequest(index);
		boolean b=client.indices().exists(grequest, RequestOptions.DEFAULT);
		if(b == true) {
			return "index allrady criated";
		}
		CreateIndexRequest request = new CreateIndexRequest(index);
		request.settings(Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 2));
		JsonNode node = new ObjectMapper().readValue(new File("E:\\vipul\\files\\indexType.json"), JsonNode.class);
		request.mapping(node.toString(), XContentType.JSON);
		//request.alias(new Alias("twitter_alias").filter(QueryBuilders.termQuery("user", "kimchy")));
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
}
