package ambition.blink.batch.table.source;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sources.BatchTableSource;
import org.apache.flink.table.sources.ProjectableTableSource;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.types.Row;

/**
 * @Author: wpl
 */
public class JsonTableSource implements BatchTableSource<Row>, ProjectableTableSource<Row> {

  private String filePath;
  private String[] fieldNames;
  private TypeInformation[] fieldTypes;

  public JsonTableSource(String filePath, String[] fieldNames,
      TypeInformation[] fieldTypes) {
    this.filePath = filePath;
    this.fieldNames = fieldNames;
    this.fieldTypes = fieldTypes;
  }

  @Override
  public DataSet<Row> getDataSet(ExecutionEnvironment execEnv) {
    return execEnv.createInput(new JsonRowInputFormat(filePath, fieldNames, fieldTypes),
        getReturnType()).name(explainSource());
  }

  @Override
  public TableSource<Row> projectFields(int[] selectedFields) {
    return new JsonTableSource(filePath, fieldNames, fieldTypes);
  }

  @Override
  public TypeInformation<Row> getReturnType() {
    return new RowTypeInfo(fieldTypes , fieldNames);
  }

  @Override
  public TableSchema getTableSchema() {
    return new TableSchema(fieldNames, fieldTypes);
  }

  @Override
  public String explainSource() {
    return String.format("JsonTableSource( read fields: %s)", fieldNames.toString());
  }
}
