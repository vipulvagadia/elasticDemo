package com.elasticsearchDemo.elasticDemo.service;

import java.io.IOException;
import java.util.Arrays;

import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.PutIndexTemplateRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.springframework.stereotype.Service;

@Service
public class ElasticsearchService {
	
	public  PutIndexTemplateRequest elasticsearc() {
		
		RestHighLevelClient client=new RestHighLevelClient(
				RestClient.builder(new HttpHost("10.1.2.15",9200,"http")));


		// index creating
		
//				CreateIndexRequest request = new CreateIndexRequest("vipul_vagadia"); 
//				request.settings(Settings.builder().put("index.number_of_shards",1).put("index.number_of_replicas",2));
//				CreateIndexResponse createIndexResponse=client.indices().create(request, RequestOptions.DEFAULT);
//				
//				System.out.println("response id: "+createIndexResponse.index());

		        PutIndexTemplateRequest request = new PutIndexTemplateRequest("my-template"); 
		        request.patterns(Arrays.asList("pattern-1", "log-*"));
		        request.mapping(
		        	    "{\n" +
		        	        "  \"properties\": {\n" +
		        	        "    \"message\": {\n" +
		        	        "      \"id\": \"long\"\n" +
		        	        "      \"enabale\": \"boolean\"\n" +
		        	        "    }\n" +
		        	        "  }\n" +
		        	        "}",
		        	    XContentType.JSON);
		        request.settings(Settings.builder().put("index.number_of_shards", 3).put("index.number_of_replicas", 1));
		        
		        return request;
	}
	public void indexTemplet() throws IOException {
		String indexName = "vipul3";
        String indexType = "companies";
        
        // Create an instance of Transport Client
        RestHighLevelClient client=new RestHighLevelClient(
    			RestClient.builder(new HttpHost("10.1.2.15",9200,"http")));
        
        
        // Create the JSON mapping
        
        String jsonMapping = "{\n" +
                          "  \"properties\": {\n" +
                          "       \"created_on\":  { \"type\": \"date\", \"format\": \"dd-MM-YYYY\" },\n" +
                          "       \"name\": { \"type\": \"text\" },\n" +
                          "       \"emp_count\": { \"type\": \"integer\" }\n" + 
                          "    }\n" +
                          " }";
        
        // Create an empty index
        CreateIndexRequest request = new CreateIndexRequest(indexName); 
		//request.settings(Settings.builder().put("index.number_of_shards",1).put("index.number_of_replicas",2));
		client.indices().create(request, RequestOptions.DEFAULT);
       
        PutMappingRequest pmr = Requests.putMappingRequest(indexName).type(indexType).source(jsonMapping, XContentType.JSON);
        client.indices().putMapping(pmr, null);
    
	}

}
