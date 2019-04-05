package ambition.blink.sql;

/**
 * @Author: wpl
 */
public interface SqlContent {
  String sqls = "CREATE FUNCTION " +
      "demouf " +
      "AS " +
      "'ambition.api.sql.function.DemoUDF' " +
      "LIBRARY " +
      "'hdfs://flink/udf/jedis.jar','hdfs://flink/udf/customudf.jar';" +

      "CREATE SOURCE TABLE csv_source (" +
      "name varchar, " +
      "amount float, " +
      "`date` date" +
      ") " +
      "with (" +
      "type=csv," +
      "'file.path'='file://demo_in.csv'" +
      ");" +

      "CREATE SINK TABLE csv_sink (" +
      "`date` date, " +
      "amount float, " +
      "PRIMARY KEY (`date`,amount)) " +
      "with (" +
      "type=csv," +
      "'file.path'='file://demo_out.csv'" +
      ");" +

      "create view view_select as  " +
      "SELECT " +
      "`date`, " +
      "amount " +
      "FROM " +
      "csv_source " +
      "group by `date`,amount;" +

      "insert " +
      "into csv_sink " +
      "SELECT " +
      "`date`, " +
      "sum(amount) " +
      "FROM " +
      "view_select " +
      "group by `date`;";
}
