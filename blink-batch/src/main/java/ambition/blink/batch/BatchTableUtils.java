package ambition.blink.batch;

import ambition.blink.batch.table.source.JsonTableSource;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.io.CsvInputFormat;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.table.sinks.BatchTableSink;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.table.sources.BatchTableSource;
import org.apache.flink.table.sources.CsvTableSource;

import java.util.Map;
import ambition.blink.common.table.TableInfo;

/**
 * @Author: wpl
 */
public class BatchTableUtils {
  static final FileSystem.WriteMode DEFAULT_WRITEMODE =FileSystem.WriteMode.OVERWRITE;
  static final int NUM_FILES =1;

  //  Only BatchTableSource can be registered in BatchTableEnvironment.
  public static BatchTableSource getBatchTableSource(TableInfo source, Map<String,String> extParams) {
    BatchTableSource result = null;
    Map<String,TypeInformation<?>> outputSchemaMap = source.getFlinkUseSchema();
    Map<String, String> kvs = source.getProps();
    String type = kvs.getOrDefault("type", "csv");

    String filePath;

    String[] fieldNames = new String[outputSchemaMap.size()];
    outputSchemaMap.keySet().toArray(fieldNames);

    TypeInformation[] fieldTypes = new TypeInformation[outputSchemaMap.size()];
    outputSchemaMap.values().toArray(fieldTypes);

    int[] selectedFields = new int[fieldNames.length];
    for (int i=0; i < fieldNames.length; i++) {
      selectedFields[i] = i;
    }

    switch (type.toUpperCase()) {
      // TXT and CSV
      case "CSV":
        filePath = kvs.getOrDefault("file.path", "/");
        String fieldDelim = kvs.getOrDefault("field.delim", CsvInputFormat.DEFAULT_FIELD_DELIMITER);
        result = new CsvTableSource(filePath, fieldNames, fieldTypes,fieldDelim,
            CsvInputFormat.DEFAULT_LINE_DELIMITER, null, false, null, false);
        ((CsvTableSource) result).projectFields(selectedFields);
        break;
      case "JSON":
        filePath = kvs.getOrDefault("file.path", "/");
        result = new JsonTableSource(filePath, fieldNames, fieldTypes);
        ((JsonTableSource) result).projectFields(selectedFields);
        break;
      default:
        break;
    }

    return result;
  }

  // Only BatchTableSink can be registered in BatchTableEnvironment.
  public static BatchTableSink getBatchTableSink(TableInfo sink, Map<String,String> extParams) {
    BatchTableSink result = null;

    Map<String, String> kvs = sink.getProps();
    String type = kvs.getOrDefault("type", "csv");

    switch (type.toUpperCase()) {
      case "CSV":
        String filePath = kvs.getOrDefault("file.path", "/");
        String fieldDelim = kvs.getOrDefault("field.delim", CsvInputFormat.DEFAULT_FIELD_DELIMITER);
        result = new CsvTableSink(filePath, fieldDelim, NUM_FILES, DEFAULT_WRITEMODE);
        break;
      default:
        break;
    }
    return result;
  }

}