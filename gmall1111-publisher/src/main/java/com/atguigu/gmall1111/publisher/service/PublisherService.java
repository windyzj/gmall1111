package com.atguigu.gmall1111.publisher.service;

import java.util.Map;

public interface PublisherService {

    public Integer   getDauTotal(String date);

    public Map getDauHour(String date);


    public Double   getOrderTotalAmount(String date);

    public Map getOrderTotalAmountHour(String date);
}
