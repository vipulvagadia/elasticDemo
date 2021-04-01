package com.elasticsearchDemo.elasticDemo.controller;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class PipelineAggregation {

	RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(new HttpHost("10.1.2.15", 9200, "http")));
	
}
