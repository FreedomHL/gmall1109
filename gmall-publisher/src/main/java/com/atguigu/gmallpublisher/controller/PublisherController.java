package com.atguigu.gmallpublisher.controller;

import com.atguigu.gmallpublisher.service.PublisherService;
import org.apache.commons.lang.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.util.*;

@RestController
public class PublisherController {

    @Autowired
    PublisherService publisherService;

    @GetMapping("realtime-total")
    public List realtime(@RequestParam("date") String date){
        //从service层获取日活总数数据
        int dauTotal = publisherService.getDauTotal(date);
        //从service层获取交易总额总数数据
        Double orderAmountTotal = publisherService.getOrderAmountTotal(date);

        //创建list集合存放最终数据
        List list = new ArrayList();

        //创建存放新增日活的map集合
        Map dauMap = new HashMap<String,Object>();
        dauMap.put("id","dau");
        dauMap.put("name","新增日活");
        dauMap.put("value",dauTotal);

        //创建存放新增设备的map集合
        Map devMap = new HashMap<String,Object>();
        devMap.put("id","new_mid");
        devMap.put("name","新增设备");
        devMap.put("value",233);

        //创建存放交易额的map集合
        Map orderMap = new HashMap<String,Object>();
        orderMap.put("id","order_amount");
        orderMap.put("name","新增交易额");
        orderMap.put("value",orderAmountTotal);

        list.add(dauMap);
        list.add(devMap);
        list.add(orderMap);
        return list;
    }

    @RequestMapping("realtime-hours")
    public Map hours(@RequestParam("id") String id,
                      @RequestParam("date") String date){
        Map todayHourMap = null;
        Map yesterdayHourMap = null;
        Map result = new HashMap<>();
        if("dau".equals(id)){
            //获取今天数据
            todayHourMap = publisherService.getDauHours(date);

            //获取昨天的日期
            String yesterday = LocalDate.parse(date).plusDays(-1).toString();

            //获取昨天数据
            yesterdayHourMap = publisherService.getDauHours(yesterday);

            result.put("today",todayHourMap);
            result.put("yesterday",yesterdayHourMap);

            return result;
        }else if("order_amount".equals(id)){
            //获取今天数据
            todayHourMap = publisherService.getOrderAmountHourMap(date);

            //获取昨天的日期
            String yesterday = LocalDate.parse(date).plusDays(-1).toString();

            //获取昨天数据
            yesterdayHourMap = publisherService.getOrderAmountHourMap(yesterday);

            result.put("today",todayHourMap);
            result.put("yesterday",yesterdayHourMap);
            return result;
        }
        return null;
    }
}
