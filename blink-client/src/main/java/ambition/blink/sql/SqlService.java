package ambition.blink.sql;

import ambition.blink.common.table.FunctionInfo;
import ambition.blink.common.table.TableInfo;
import ambition.blink.common.table.ViewOrInsertInfo;

import java.util.List;
import java.util.Map;

/**
 * @Author: wpl
 */
public interface SqlService {
   /**
    * @param sqlContext    sql语句集合
    * @return sql语句分类对应的集合
    * @throws Exception
    */
   Map<String,List<String>> sqlConvert(String sqlContext) throws Exception;

   /**
    * function语句解析
    * @param funSql
    * @return
    * @throws Exception
    */
   FunctionInfo sqlFunctionParser(String funSql) throws Exception;

   /**
    * ddl语句解析
    * @param ddlSql
    * @return
    * @throws Exception
    */
   TableInfo sqlTableParser(String ddlSql) throws Exception;

   /**
    * sql中视图sql解析
    * @param viewSql
    * @return sql的封装信息类
    * @throws Exception
    */
   ViewOrInsertInfo sqlViewParser(String viewSql) throws Exception;

   /**
    * sql中的insert into内容
    * @param insertSql
    * @return
    * @throws Exception
    */
   ViewOrInsertInfo sqlDmlParser(String insertSql) throws Exception;
}
