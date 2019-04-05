package ambition.blink.job;

import ambition.blink.batch.job.impl.BatchJobClientImpl;
import ambition.blink.common.job.JobParameter;
import ambition.blink.sql.SqlService;
import ambition.blink.sql.impl.SqlServiceImpl;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.Plan;
import org.apache.flink.client.LocalExecutor;

/**
 * @Author: wpl
 */
public class BatchJobLocalRunExample {

  public static void main(String[] args) throws Exception {
    JobClient jobClient = new BatchJobClientImpl();
    JobParameter jobParameter = new JobParameter();
    SqlService sqlService = new SqlServiceImpl();
    Map<String, List<String>> map = sqlService.sqlConvert(sqls);
    jobParameter.setSqls(map);
    jobParameter.setJobName("batch_test");

    LocalExecutor executor = new LocalExecutor();
    executor.setDefaultOverwriteFiles(true);
    executor.setTaskManagerNumSlots(1);
    executor.setPrintStatusDuringExecution(false);
    executor.start();
    Plan jobPlan = jobClient.getJobPlan(jobParameter, null);
    jobPlan.setExecutionConfig(new ExecutionConfig());
    executor.executePlan(jobPlan);
    executor.stop();
  }

  private String getPath(String fileName) {
    return getClass().getClassLoader().getResource(fileName).getPath();
  }

  static String sqls =
      "CREATE SOURCE TABLE csv_source (" +
      "id int, " +
      "name varchar, " +
      "`date` date , " +
      "age int" +
      ") " +
      "with (" +
      "type=json," +
      "'file.path'='file:///FlinkSQL/blink-job/src/test/resources/demo.json'" +
      ");" +

      "CREATE SINK TABLE csv_sink (" +
      "`date` date, " +
      "age int, " +
      "PRIMARY KEY (`date`)) " +
      "with (" +
      "type=csv," +
      "'file.path'='file:///FlinkSQL/blink-job/src/test/resources/demo_out.csv'" +
      ");" +

      "create view view_select as  " +
      "SELECT " +
      "`date`, " +
      "age " +
      "FROM " +
      "csv_source " +
      "group by `date`,age;" +

      "insert " +
      "into csv_sink " +
      "SELECT " +
      "`date`, " +
      "sum(age) " +
      "FROM " +
      "view_select " +
      "group by `date`;";

}
