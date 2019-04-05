package ambition.blink.sql.impl;


import ambition.blink.sql.SqlContent;
import ambition.blink.sql.SqlConstant;
import java.util.List;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;
import ambition.blink.sql.SqlService;
import ambition.blink.common.table.FunctionInfo;
import ambition.blink.common.table.TableInfo;
import ambition.blink.common.table.ViewOrInsertInfo;

/**
 * @Author: wpl
 */
public class SqlServiceImplTest {
  private String sqlContext = SqlContent.sqls;
  private SqlService sqlService =  new SqlServiceImpl();

  @Test
  public void sqlConvertTest() throws Exception {
    Map<String, List<String>> map = sqlService.sqlConvert(sqlContext);

    Assert.assertEquals(map.size(), 5);
  }

  @Test
  public void sqlFunctionParserTest() throws Exception {
    Map<String, List<String>> map = sqlService.sqlConvert(sqlContext);
    List<String> fun = map.get(SqlConstant.FUNCTION);
    Assert.assertNotNull(fun);

    for (String sql: fun) {
      FunctionInfo functionInfo = sqlService.sqlFunctionParser(sql);
      Assert.assertNotNull(functionInfo);
    }
  }

  @Test
  public void sqlTableParserTest() throws Exception {
    Map<String, List<String>> map = sqlService.sqlConvert(sqlContext);
    List<String> source = map.get(SqlConstant.TABLE_SOURCE);
    Assert.assertNotNull(source);
    for (String sql: source) {
      TableInfo tableInfo = sqlService.sqlTableParser(sql);
      Assert.assertNotNull(tableInfo);
    }

    List<String> sink = map.get(SqlConstant.TABLE_SINK);
    for (String sql: sink) {
      TableInfo tableInfo = sqlService.sqlTableParser(sql);
      Assert.assertNotNull(tableInfo);
    }
  }

  @Test
  public void sqlViewParserTest() throws Exception {
    Map<String, List<String>> map = sqlService.sqlConvert(sqlContext);
    List<String> view = map.get(SqlConstant.TABLE_VIEW);
    for (String sql: view) {
      ViewOrInsertInfo viewInfo = sqlService.sqlViewParser(sql);
      Assert.assertNotNull(viewInfo);
    }
  }

  @Test
  public void sqlDmlParser() throws Exception {
    Map<String, List<String>> map = sqlService.sqlConvert(sqlContext);
    List<String> insert = map.get(SqlConstant.INSERT_INTO);
    for (String sql: insert) {
      ViewOrInsertInfo insertInfo = sqlService.sqlDmlParser(sql);
      Assert.assertNotNull(insertInfo);
    }
  }

}
