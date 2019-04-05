package ambition.blink.stream;

import java.util.Map;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.TableSource;
import ambition.blink.common.table.TableInfo;

/**
 * @Author: wpl
 */
public class StreamTableUtils {

  public static TableSource getTableSource(TableInfo source, Map<String,String> extParams) {
    TableSource result = null;
    Map<String,TypeInformation<?>> outputSchemaMap = source.getFlinkUseSchema();
    Map<String, String> kvs = source.getProps();
    String type = kvs.getOrDefault("type", "kafka");

    switch (type.toUpperCase()) {
      case "KAFKA":
        break;
      default:
        break;
    }

    return result;
  }

  public static TableSink getTableSink(TableInfo sink, Map<String,String> extParams) {
    TableSink result = null;

    Map<String, String> kvs = sink.getProps();
    String type = kvs.getOrDefault("type", "kafka");

    switch (type.toUpperCase()) {
      case "KAFKA":

        break;
      default:
        break;
    }
    return result;
  }

}
