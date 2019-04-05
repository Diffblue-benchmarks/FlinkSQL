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

      "CREATE SOURCE TABLE kafak_source (" +
      "name varchar, " +
      "amount float, " +
      "`date` date," +
      "watermark for date AS withOffset(date,1000) " +
      ") " +
      "with (" +
      "type=kafka," +
      "'flink.parallelism'=1," +
      "'kafka.topic'='topic'," +
      "'kafka.group.id'='flinks'," +
      "'kafka.enable.auto.commit'=true," +
      "'kafka.bootstrap.servers'='localhost:9092'" +
      ");" +

      "CREATE SINK TABLE mysql_sink (" +
      "`date` date, " +
      "amount float, " +
      "PRIMARY KEY (`date`,amount)) " +
      "with (" +
      "type=mysql," +
      "'mysql.connection'='localhost:3306'," +
      "'mysql.db.name'=flink," +
      "'mysql.batch.size'=0," +
      "'mysql.table.name'=flink_table," +
      "'mysql.user'=root," +
      "'mysql.pass'=root" +
      ");" +

      "create view view_select as  " +
      "SELECT " +
      "`date`, " +
      "amount " +
      "FROM " +
      "kafak_source " +
      "group by `date`,amount;" +

      "insert " +
      "into mysql_sink " +
      "SELECT " +
      "`date`, " +
      "sum(amount) " +
      "FROM " +
      "view_select " +
      "group by `date`;";
}
