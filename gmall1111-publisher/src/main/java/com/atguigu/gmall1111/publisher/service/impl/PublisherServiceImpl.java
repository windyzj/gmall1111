package com.atguigu.gmall1111.publisher.service.impl;

import com.atguigu.gmall1111.common.constant.GmallConstant;
import com.atguigu.gmall1111.publisher.service.PublisherService;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class PublisherServiceImpl implements PublisherService {

    @Autowired
    JestClient jestClient;

    /**
     * 从es中查询当日日活总数
     * @param date
     * @return
     */
    @Override
    public Integer getDauTotal(String date) {

      String query=  "{\n" +
                "  \"query\":{\n" +
                "    \"bool\": {\n" +
                "      \"filter\": {\n" +
                "        \"term\": {\n" +
                "          \"logDate\": \""+date+"\"\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}";

        Search search = new Search.Builder(query).addIndex(GmallConstant.ES_INDEX_DAU).addType(GmallConstant.ES_TYPE_DEFAULT).build();
        Integer total=0;
        try {
            SearchResult searchResult = jestClient.execute(search);
            total=searchResult.getTotal();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return total;
    }


    /**
     * 日活分时统计
     * @param date
     * @return
     */
    @Override
    public Map getDauHour(String date) {
        HashMap<String, Long> dauHourMap = new HashMap<>();

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //按日期进行过滤
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.filter(new TermQueryBuilder("logDate",date));
        searchSourceBuilder.query(boolQueryBuilder);
        //按小时进行分组聚合
        TermsBuilder termAggs = AggregationBuilders.terms("groupby_logHour").field("logHour").size(24);
        searchSourceBuilder.aggregation(termAggs);

        System.out.println(searchSourceBuilder.toString());
        Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex(GmallConstant.ES_INDEX_DAU).addType(GmallConstant.ES_TYPE_DEFAULT).build();
        try {
            SearchResult searchResult = jestClient.execute(search);
            //循环把分时结果保存到map中
            List<TermsAggregation.Entry> buckets = searchResult.getAggregations().getTermsAggregation("groupby_logHour").getBuckets();
            for (TermsAggregation.Entry bucket : buckets) {
                dauHourMap.put( bucket.getKey(),bucket.getCount());
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
        return dauHourMap;
    }
}
