package com.wojustme.goblin.sql;

import org.apache.calcite.rel.RelNode;
import org.testng.annotations.Test;

public class ParserTest extends GoblinSqlBaseTest {

  @Test
  public void testName() {
    String sql =
        "SELECT  to_char(curr_month , 'yyyy-MM')     AS curr_month\n"
            + "        ,curr_num\n"
            + "        ,last_num\n"
            + "        ,( curr_num - last_num ) / last_num AS ratio\n"
            + "FROM    (\n"
            + "            SELECT  curr_month\n"
            + "                    ,CAST(curr_num AS DOUBLE)                                                               AS curr_num\n"
            + "                    ,CAST(lag(curr_num , 12 , CAST(NULL AS DOUBLE)) OVER ( ORDER BY curr_month ) AS DOUBLE) AS last_num\n"
            + "            FROM    (\n"
            + "                        SELECT  curr_month\n"
            + "                                ,sum(order_num) AS curr_num\n"
            + "                        FROM    (\n"
            + "                                    SELECT  order_time AS curr_month\n"
            + "                                            ,order_num\n"
            + "                                    FROM    ( VALUES\n"
            + "( '0571' , 10 , to_date('2022-01' , 'yyyy-MM') ) ,\n"
            + "( '0571' , 2 , to_date('2022-02' , 'yyyy-MM') ) ,\n"
            + "( '010' , 11 , to_date('2022-03' , 'yyyy-MM') ) ,\n"
            + "( '0571' , 11 , to_date('2022-03' , 'yyyy-MM') ) ,\n"
            + "( '0571' , 13 , to_date('2022-04' , 'yyyy-MM') ) ,\n"
            + "( '0571' , 10 , to_date('2022-05' , 'yyyy-MM') ) ,\n"
            + "( '0571' , 14 , to_date('2022-06' , 'yyyy-MM') ) ,\n"
            + "( '0571' , 210 , to_date('2022-07' , 'yyyy-MM') ) ,\n"
            + "( '0571' , 120 , to_date('2022-08' , 'yyyy-MM') ) ,\n"
            + "( '0571' , 103 , to_date('2022-09' , 'yyyy-MM') ) ,\n"
            + "( '0571' , 102 , to_date('2022-10' , 'yyyy-MM') ) ,\n"
            + "( '0571' , 108 , to_date('2022-11' , 'yyyy-MM') ) ,\n"
            + "( '0571' , 19 , to_date('2022-11' , 'yyyy-MM') ) ,\n"
            + "( '0571' , 12 , to_date('2022-12' , 'yyyy-MM') ) ,\n"
            + "( '0571' , 11 , to_date('2023-01' , 'yyyy-MM') ) ) AS t ( city_id , order_num , order_time )\n"
            + "                                )\n"
            + "                        GROUP BY curr_month\n"
            + "                    )\n"
            + "        )";
    System.out.println(sqlPlanner.toRel(sql).explain());
  }

  @Test
  public void testToDate() {
    String sql = "select to_date('2022-11', `pattern`)\n"
            + "from (values('yyyy-MM')) as t(`pattern`)";
//    String sql = "select to_date(str, 'yyyy-MM')\n"
//            + "from (values('2022-11')) as t(str)";
    final RelNode rel = sqlPlanner.toRel(sql);
    System.out.println(rel.explain());
    System.out.println(rel.getRowType().getFullTypeString());
  }
}
