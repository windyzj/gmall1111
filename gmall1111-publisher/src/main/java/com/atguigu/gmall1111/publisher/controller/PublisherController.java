package com.atguigu.gmall1111.publisher.controller;


import com.alibaba.fastjson.JSON;
import com.atguigu.gmall1111.publisher.bean.Option;
import com.atguigu.gmall1111.publisher.bean.OptionGroup;
import com.atguigu.gmall1111.publisher.bean.SaleInfo;
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

        HashMap<String, Object> orderAmountMap = new HashMap<>();
        orderAmountMap.put("id","order_amount");
        orderAmountMap.put("name","新增交易额");
        Double orderTotalAmount = publisherService.getOrderTotalAmount(date);
        orderAmountMap.put("value",orderTotalAmount);
        totalList.add(orderAmountMap);


        return   JSON.toJSONString(totalList) ;
    }

    /**
     * 分时数据
     * @param id
     * @param today
     * @return
     */
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

        }else if("order_amount".equals(id)){
            //查询今日分时
            Map orderAmountHourTdMap = publisherService.getOrderTotalAmountHour(today);
            //查询昨日分时
            String yesterday = getYesterday(today);
            Map orderAmountHourYdMap = publisherService.getOrderTotalAmountHour(yesterday);

            HashMap<String, Map> realtimeHourMap = new HashMap<>();
            realtimeHourMap.put("yesterday",orderAmountHourYdMap);
            realtimeHourMap.put("today",orderAmountHourTdMap);
            hourJson=JSON.toJSONString(realtimeHourMap);
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

    @GetMapping("sale_detail")
    public String getSaleDetail(@RequestParam("date") String date,@RequestParam("keyword") String keyword,@RequestParam("startpage") int startPage, @RequestParam("size") int size){
        SaleInfo saleInfoWithGenderAggs = publisherService.getSaleInfo(date, keyword, startPage, size, "user_gender", 2);
        Integer total = saleInfoWithGenderAggs.getTotal();
        Map genderAggsMap = saleInfoWithGenderAggs.getTempAggsMap();

        Long maleCount = (Long)genderAggsMap.getOrDefault("M", 0L);
        Long femaleCount = (Long)genderAggsMap.getOrDefault("F", 0L);

        Double maleRatio=   Math.round(maleCount*1000D/total )/10D;
        Double femaleRatio=   Math.round(femaleCount*1000D/total )/10D;

        List<Option> genderOptionList=new ArrayList<>();  //选项列表
        genderOptionList.add(new Option("男", maleRatio));
        genderOptionList.add(new Option("女", femaleRatio));

        List<OptionGroup> optionGroupList=  new ArrayList<>(); //饼图列表
        optionGroupList.add( new OptionGroup(genderOptionList,"性别占比")) ;



        SaleInfo saleInfoWithAgeAggs = publisherService.getSaleInfo(date, keyword, startPage, size, "user_age", 100);
        Map ageAggsMap = saleInfoWithAgeAggs.getTempAggsMap();
        //通过每个年龄的计数清单 计算各个年龄段的占比

        Long age_20Count=0L;
        Long age20_30Count=0L;
        Long age30_Count=0L;
        for (Object o : ageAggsMap.entrySet()) {
            Map.Entry entry = (Map.Entry) o;
            String ageStr =(String) entry.getKey();
            Integer age=Integer.parseInt(ageStr);

            Long count =(Long) entry.getValue();
            if(age<20){
                age_20Count+=count;
            }else if(age>=20&&age<30){
                age20_30Count+=count;
            }else{
                age30_Count+=count;
            }

        }
        Double age_20Ratio=0D;
        Double age20_30Ratio=0D;
        Double age30_Ratio=0D;

        age_20Ratio=Math.round(age_20Count*1000D/total )/10D;
        age20_30Ratio=Math.round(age20_30Count*1000D/total )/10D;
        age30_Ratio=Math.round(age30_Count*1000D/total )/10D;

        List<Option> ageOptionList=new ArrayList<>();  //选项列表
        ageOptionList.add(new Option("小于20岁", age_20Ratio));
        ageOptionList.add(new Option("20至30岁", age20_30Ratio));
        ageOptionList.add(new Option("30岁及以上", age30_Ratio));

         optionGroupList.add( new OptionGroup(ageOptionList,"年龄占比"));


        SaleInfo saleInfo = new SaleInfo();
        saleInfo.setTotal(total);
        saleInfo.setDetail(saleInfoWithGenderAggs.getDetail());
        saleInfo.setStat(optionGroupList);


       return JSON.toJSONString(saleInfo);

    }

}
