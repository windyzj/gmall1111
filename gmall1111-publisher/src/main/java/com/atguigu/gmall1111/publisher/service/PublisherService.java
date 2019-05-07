package com.atguigu.gmall1111.publisher.service;

import com.atguigu.gmall1111.publisher.bean.SaleInfo;

import java.util.Map;

public interface PublisherService {

    public Integer   getDauTotal(String date);

    public Map getDauHour(String date);


    public Double   getOrderTotalAmount(String date);

    public Map getOrderTotalAmountHour(String date);

    public SaleInfo getSaleInfo(String date, String keyword, int startPage, int pagesize, String aggsFieldName, int aggsize);
}
