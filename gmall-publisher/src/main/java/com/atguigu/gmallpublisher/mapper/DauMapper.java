package com.atguigu.gmallpublisher.mapper;

import java.util.List;
import java.util.Map;

public interface DauMapper {
    //查询当日dau总数
    public Integer selectDauTotal(String date);
    //查询当日dau分时明细
    public List<Map> selectDauTotalHourMap(String date);

}
