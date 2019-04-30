package com.atguigu.gmall1111.publisher.controller;


import com.alibaba.fastjson.JSON;
import com.atguigu.gmall1111.publisher.service.PublisherService;
import org.apache.commons.lang.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

@RestController
public class PublisherController {

    @Autowired
    PublisherService publisherService;

    @GetMapping("realtime-total")
    public String getRealtimeTotal(@RequestParam("date") String date){

        List<Map> totalList = new ArrayList<>();

        HashMap<String, Object> dauMap = new HashMap<>();
        dauMap.put("id","dau");
        dauMap.put("name","新增日活");
        Integer dauTotal = publisherService.getDauTotal(date);
        dauMap.put("value",dauTotal);
        totalList.add(dauMap);

        HashMap<String, Object> newMidMap = new HashMap<>();
        newMidMap.put("id","new_mid");
        newMidMap.put("name","新增设备");
        newMidMap.put("value",233);
        totalList.add(newMidMap);


        return   JSON.toJSONString(totalList) ;
    }

    @GetMapping("realtime-hour")
    public String getRealtimeHour(@RequestParam("id") String id ,@RequestParam("date") String today){
        String hourJson=null;
        if("dau".equals(id)){
            //查询今日分时
            Map dauHourTdMap = publisherService.getDauHour(today);
            //查询昨日分时
            String yesterday = getYesterday(today);
            Map dauHourYdMap = publisherService.getDauHour(yesterday);

            HashMap<String, Map> realtimeHourMap = new HashMap<>();
            realtimeHourMap.put("yesterday",dauHourYdMap);
            realtimeHourMap.put("today",dauHourTdMap);
            hourJson=JSON.toJSONString(realtimeHourMap);

        }else {
            //xxxx
        }
       return hourJson;
    }

    public String getYesterday(String today){
        Date todayDt=new Date();
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        try {
              todayDt = simpleDateFormat.parse(today);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        Date yesterdayDt = DateUtils.addDays(todayDt, -1);
        String yesterday = simpleDateFormat.format(yesterdayDt);
        return yesterday;

    }


}
