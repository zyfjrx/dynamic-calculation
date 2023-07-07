package com.byt.common.utils;

import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.MapListHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;


public class QueryDbUtil {
    private static final Logger LOGGER = LoggerFactory.getLogger(QueryDbUtil.class);
    private static final long serialVersionUID = 1L;
    private static Map<String, Map<String, Object>> cacheMap;

    /**
     * 获取单个job配置
     * @param jobName
     * @return
     */
    public static String getSql(String jobName) {
        return "SELECT * from byt_grid_data.tag_conf where task_name='" + jobName.trim() + "'";
    }

    /**
     * 获取多个job的配置
     * @param jobName
     * @return
     */
    public static String getSqlWithJobs(String jobNames) {
        String[] jobarry = jobNames.split(",");
        StringBuilder sql = new StringBuilder("( ");
        for (int i = 0; i < jobarry.length; i++) {
            sql.append("'").append(jobarry[i]).append("'");
            if (i < jobarry.length - 1) {
                sql.append(",");
            }
        }
        sql.append(" )");
        return "SELECT * from byt_grid_data.tag_conf where task_name in " + sql.toString() ;
    }

    public static String getSourceTopicSql(String jobNames) {
        String[] jobarry = jobNames.split(",");
        StringBuilder sd = new StringBuilder("( ");
        for (int i = 0; i < jobarry.length; i++) {
            sd.append("'").append(jobarry[i]).append("'");
            if (i < jobarry.length - 1) {
                sd.append(",");
            }
        }
        sd.append(" )");
        //System.out.println(sd.toString());
        return "SELECT * from byt_grid_data.jobs where task_name in " + sd.toString();
    }




    /**
     * 获取除了秒级别以外所有的数据
     *
     * @param jobName
     * @return 参数集合
     */
    public static Set<String> getParams(String jobName) {
        String sql = getSql(jobName);
        System.out.println(sql);
        Set<String> paramSet = new HashSet<>();
        try {
            QueryRunner queryRunner = new QueryRunner(JdbcUtil.getDataSource());
            List<Map<String, Object>> info = queryRunner.query(sql, new MapListHandler());

            for (Map<String, Object> item : info) {
                if (item.get("calculate_type") == null) {
                    continue;
                }
                String calParamsKay = "";
                String timeGap = (String) item.get("param");
                if (timeGap.equals("")) {
                    timeGap = "t";
                }
                if (!timeGap.contains("s")) {
                    calParamsKay = item.get("calculate_type") + "#" + timeGap;
                }
                if (calParamsKay != "") {
                    paramSet.add(calParamsKay);
                }
            }
            LOGGER.info("Fetched {} site info records, {} records in cache", info.size(), paramSet.size());
        } catch (Exception e) {
            LOGGER.error("Exception occurred when querying: " + e);
        }
        return paramSet;
    }

    @Deprecated
    public static Set<String> getOldParams(String jobName) {
        String sql = getSqlWithJobs(jobName);
        Set<String> paramSet = new HashSet<>();
        try {
            QueryRunner queryRunner = new QueryRunner(JdbcUtil.getDataSource());
            List<Map<String, Object>> info = queryRunner.query(sql, new MapListHandler());

            for (Map<String, Object> item : info) {
                if (item.get("calculate_type") == null) {
                    continue;
                }
                String calParamsKay;
                if (item.get("slide_gap") == null) {
                    calParamsKay = item.get("calculate_type") + "#" + item.get("param") + "#" + "";
                } else {
                    calParamsKay = item.get("calculate_type") + "#" + item.get("param") + "#" + item.get("slide_gap");
                }
                paramSet.add(calParamsKay);
            }
            LOGGER.info("Fetched {} site info records, {} records in cache", info.size(), paramSet.size());
        } catch (Exception e) {
            LOGGER.error("Exception occurred when querying: " + e);
        }
        return paramSet;
    }

    public static Set<String> getParamsWithJobs(String jobNames) {
        String sql = getSqlWithJobs(jobNames);
        System.out.println(sql);
        Set<String> paramSet = new HashSet<>();
        try {
            QueryRunner queryRunner = new QueryRunner(JdbcUtil.getDataSource());
            List<Map<String, Object>> info = queryRunner.query(sql, new MapListHandler());

            for (Map<String, Object> item : info) {
                if (item.get("calculate_type") == null) {
                    continue;
                }
                String calParamsKay = "";
                String timeGap = (String) item.get("param");
                if (timeGap.equals("")) {
                    timeGap = "t";
                }
                // dev
                // if (!timeGap.contains("m")) {
                calParamsKay = item.get("calculate_type") + "#" + timeGap;
                // }
                if (calParamsKay != "") {
                    paramSet.add(calParamsKay);
                }
            }
            LOGGER.info("Fetched {} site info records, {} records in cache", info.size(), paramSet.size());
        } catch (Exception e) {
            LOGGER.error("Exception occurred when querying: " + e);
        }
        return paramSet;
    }

    public static void main(String[] args) {
/*        Set<String> params = getParams("job1");
        System.out.println(Arrays.toString(params.toArray()));
        HashMap<String, OutputTag<TagKafkaInfo>> sideOutPutTags = SideOutPutTagUtil.getSideOutPutTags(params);
        System.out.println(sideOutPutTags);*/
        // System.out.println(getSourceTopicSql("job1,job2"));
        //System.out.println(getTopicMap(args[0]));
        System.out.println(getParamsWithJobs("job1,job2"));
    }
}
