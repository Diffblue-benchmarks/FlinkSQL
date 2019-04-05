package ambition.blink.common.table;

/**
 * @Author: wpl
 */
public class ViewOrInsertInfo {
  private String tableName;

  private String fieldsInfoStr;

  private String execSql;

  public ViewOrInsertInfo() {
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public String getFieldsInfoStr() {
    return fieldsInfoStr;
  }

  public void setFieldsInfoStr(String fieldsInfoStr) {
    this.fieldsInfoStr = fieldsInfoStr;
  }

  public String getExecSql() {
    return execSql;
  }

  public void setExecSql(String execSql) {
    this.execSql = execSql;
  }
}
