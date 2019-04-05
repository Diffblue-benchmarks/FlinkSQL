package ambition.blink.sql.parser;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import ambition.blink.common.table.FunctionInfo;

/**
 * @Author: wpl
 */
public class CreateFuncParser {

    private static final String funcPatternStr = "(?i)\\s*create\\s+function\\s+(\\S+)\\s+as\\s+(\\S+)\\s+library\\s+(\\S+)";

    private static final Pattern funcPattern = Pattern.compile(funcPatternStr);

    public FunctionInfo parseSql(String sql) {
        FunctionInfo result = null;
        Matcher matcher = funcPattern.matcher(sql);
        if(matcher.find()){
            String funcName = matcher.group(1);
            String className = matcher.group(2);
            String jarPath = matcher.group(3);
            result = new FunctionInfo();
            result.setName(funcName);
            result.setClassName(className);
            result.setJarPath(jarPath);
        }
        return result;
    }


    public static CreateFuncParser newInstance(){
        return new CreateFuncParser();
    }
}
