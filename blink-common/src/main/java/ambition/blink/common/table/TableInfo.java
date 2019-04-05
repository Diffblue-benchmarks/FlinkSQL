package ambition.blink.common.table;

import java.util.Map;
import org.apache.flink.api.common.typeinfo.TypeInformation;

/**
 * @Author: wpl
 */
public class TableInfo {
   private String tableName;
   private TableType tableType;
   private String primarys;

   //字段和类型
   private Map<String,String> inputSchema;

   //kv键值对，props信息
   private Map<String,String> props;
   private Map<String,String> virtuals ;
   private Map<String,String> watermarks;

   //字段和类型
   private Map<String,TypeInformation<?>> flinkUseSchema;

   public TableInfo() {
   }

   public String getTableName() {
      return tableName;
   }

   public void setTableName(String tableName) {
      this.tableName = tableName;
   }

   public TableType getTableType() {
      return tableType;
   }

   public void setTableType(TableType tableType) {
      this.tableType = tableType;
   }

   public String getPrimarys() {
      return primarys;
   }

   public void setPrimarys(String primarys) {
      this.primarys = primarys;
   }

   public Map<String, String> getInputSchema() {
      return inputSchema;
   }

   public void setInputSchema(Map<String, String> inputSchema) {
      this.inputSchema = inputSchema;
   }

   public Map<String, String> getProps() {
      return props;
   }

   public void setProps(Map<String, String> props) {
      this.props = props;
   }

   public Map<String, String> getVirtuals() {
      return virtuals;
   }

   public void setVirtuals(Map<String, String> virtuals) {
      this.virtuals = virtuals;
   }

   public Map<String, String> getWatermarks() {
      return watermarks;
   }

   public void setWatermarks(Map<String, String> watermarks) {
      this.watermarks = watermarks;
   }

   public Map<String, TypeInformation<?>> getFlinkUseSchema() {
      return flinkUseSchema;
   }

   public void setFlinkUseSchema(Map<String, TypeInformation<?>> flinkUseSchema) {
      this.flinkUseSchema = flinkUseSchema;
   }
}
