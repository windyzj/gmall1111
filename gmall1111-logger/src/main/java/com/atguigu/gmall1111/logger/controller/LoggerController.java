package com.atguigu.gmall1111.logger.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall1111.common.constant.GmallConstant;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

@RestController  //== @Controller+@ResponseBody
public class LoggerController {

    @Autowired
    KafkaTemplate kafkaTemplate;

    private static final  org.slf4j.Logger logger = LoggerFactory.getLogger(LoggerController.class) ;

    @PostMapping("log")
    public String doLog(@RequestParam("log") String log){

        JSONObject jsonObject = JSON.parseObject(log);
        jsonObject.put("ts",System.currentTimeMillis());

        // 落盘成为日志文件     // log4j
        logger.info (jsonObject.toJSONString());
        // 发送kafka
        if("startup".equals(  jsonObject.getString("type"))){
            kafkaTemplate.send(GmallConstant.KAFKA_TOPIC_STARTUP,jsonObject.toJSONString());
        }else{
            kafkaTemplate.send(GmallConstant.KAFKA_TOPIC_EVENT,jsonObject.toJSONString());
        }

        return "success";
    }

}
