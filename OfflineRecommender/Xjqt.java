package com.chanct.kaitong.start;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataTypes;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * Copyright (C), 97tech,
 * FileName: Xjqt
 * Date:     2020/9/9 10:49
 * Description: $新疆群体分析
 */
public class Xjqt {
    public static void main(String[] args) {
        if (args.length < 4){
            System.exit(0);
        }
        String startDate = args[0];
        String endDate = args[1];
        String midTable = args[2];
        String aimTable = args[3];

        SparkSession session = SparkSession.builder().appName("SzStatistics").enableHiveSupport().getOrCreate();
        JavaSparkContext jsc = new JavaSparkContext(session.sparkContext());

        HiveContext hiveContext = new HiveContext(jsc);
        hiveContext.setConf("hive.exec.dynamic.partition", "true");
        hiveContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict");

        session.udf().register("isChinaMsisdn", new UDF1<String, Boolean>() {
            @Override
            public Boolean call(String s) throws Exception {

                if (s == null || "".equals(s)){
                    return  false;
                }else {
                    return  s.matches("^(86)?(1[38][0-9]|14[5-9]|15[0-35-9]|166|17[0-8]|19[026789])\\d{8}$");
                }

            }
        }, DataTypes.BooleanType);

        Dataset<Row> dataset = session.sql("select * from tools.hcode_small");
        dataset.cache();
        dataset.createOrReplaceTempView("hcodesmall");

        //日期函数
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        try {
            Date start = sdf.parse(startDate);
            Date end = sdf.parse(endDate);
            Date temp = start;
            Calendar instance = Calendar.getInstance();
            instance.setTime(start);

            while (temp.getTime() < end.getTime()){

                temp = instance.getTime();
                String s = sdf.format(temp);
                getResult(session,s,midTable,aimTable);
                instance.add(Calendar.DAY_OF_MONTH,1);
            }

        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

    public static void getResult(SparkSession session,String s,String table1,String table2){

        session.sql("select * from mdatabase.t_people_snapshot where day = "+s+" and c_areacode like '4403%' and c_homecode in ('0901','0902','0903','0906','0908','0909','0991','0992','0993','0994','0995','0996','0997','0998','0999')  ").createOrReplaceTempView("t_person_xj");
        session.sql("insert into "+table1+" partition(day,hour) select * from t_person_xj");
        session.sql("insert into "+table2+" select distinct c_msisdn,"+s+" from "+table1+" where day = "+s+" ");

    }
}
