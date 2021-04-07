package com.atguigu.gmallpublisher.service;

import com.atguigu.gmallpublisher.mapper.DauMapper;
import com.atguigu.gmallpublisher.mapper.OrderMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class PublisherServiceImpl implements PublisherService {

    @Autowired
    DauMapper dauMapper;

    @Autowired
    OrderMapper orderMapper;

    //每日DAU总数
    @Override
    public int getDauTotal(String date) {
        return dauMapper.selectDauTotal(date);
    }
    //每日Dau分时
    @Override
    public Map getDauHours(String date) {
        Map mp = new HashMap();
        List<Map> maps = dauMapper.selectDauTotalHourMap(date);
        for(Map map : maps){
            mp.put(map.get("LH"),map.get("CT"));
        }
        return mp;
    }

    //每日交易额总数
    @Override
    public Double getOrderAmountTotal(String date) {
        return orderMapper.selectOrderAmountTotal(date);
    }

    //每日交易额分时
    @Override
    public Map getOrderAmountHourMap(String date) {
        Map mp = new HashMap();
        List<Map> maps = orderMapper.selectOrderAmountHourMap(date);
        for(Map map : maps){
            mp.put(map.get("CREATE_HOUR").toString().split(":")[0],map.get("SUM_AMOUNT"));
        }
        return mp;
    }
}
