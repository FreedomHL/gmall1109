package com.atguigu.gmallpublisher.service;

import java.util.Map;

public interface PublisherService {
    public int getDauTotal(String date);

    public Map getDauHours(String date);

    public Double getOrderAmountTotal(String date);

    public Map getOrderAmountHourMap(String date);
}
