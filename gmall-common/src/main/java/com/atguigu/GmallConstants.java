package com.atguigu;

public class GmallConstants {
    //启动数据主题
    public static final String KAFKA_TOPIC_STARTUP="GMALL_STARTUP";

    //订单主题
    public static final String KAFKA_TOPIC_ORDER="GMALL_ORDER";

    //事件数据主题
    public static final String KAFKA_TOPIC_EVENT="GMALL_EVENT";

    //预警需求索引名
    public static final String ES_ALERT_INDEXNAME = "gmall_coupon_alert";

    public static final String KAFKA_TOPIC_NEW_ORDER="GMALL_NEW_ORDER";
    public static final String KAFKA_TOPIC_ORDER_DETAIL="GMALL_ORDER_DETAIL";

    public static final String ES_INDEX_DAU="gmall1109_dau";
    public static final String ES_INDEX_NEW_MID="gmall1109_new_mid";
    public static final String ES_INDEX_NEW_ORDER="gmall1109_new_order";
    public static final String ES_INDEX_SALE_DETAIL="gmall1109_sale_detail";
}
