package sqlancer.common;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import sqlancer.GlobalState;

public class AbstractMutator {
    String sql;
    GlobalState state;
    public AbstractMutator(GlobalState globalState, String sql) {
        this.sql = sql;
        this.state = globalState;
    }
    public List<String> extract_column_name_from_stmt(String sql) {
        // 正则表达式：匹配以 c 开头，后面跟数字的列名
        String regex = "\\bc\\d+\\b"; // \b 表示单词边界，以避免匹配到类似 t1234abc 的表名
        
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(sql);
        
        List<String> tableNames = new ArrayList<>();
        
        while (matcher.find()) {
            tableNames.add(matcher.group());
        }
        tableNames = tableNames.stream()
                                .distinct()
                                .collect(Collectors.toList());
        return tableNames;
    }
    public String getRandomColumnStringUsingRegex() {
        List<String> cols = extract_column_name_from_stmt(sql);
        if(cols.size() == 0) return null;
        return state.getRandomly().fromList(cols);
    }
    private int find_insert_pos() {
        int pos = sql.length();
        

        if(sql.toLowerCase().contains("limit")) {
            pos = Math.min(pos, sql.toLowerCase().lastIndexOf("limit"));
        }
        if(sql.toLowerCase().contains("offset")) {
            pos = Math.min(pos, sql.toLowerCase().lastIndexOf("offset"));
        }
        if(sql.toLowerCase().contains("group by")) {
            pos = Math.min(pos, sql.toLowerCase().lastIndexOf("group by"));
        }
        if(sql.toLowerCase().contains("order by")) {
            pos = Math.min(pos, sql.toLowerCase().lastIndexOf("order by"));
        }
        return pos;
    }
    public String mutateDQL() {
        
        String col = getRandomColumnStringUsingRegex();
        if(col == null) {
            return sql;
        }
        if(sql.endsWith(";")) {
            sql = sql.substring(0, sql.length() - 1);
        }
        int pos_to_insert = find_insert_pos();
        String suffix = "";
        if(pos_to_insert != sql.length()) {
            suffix = sql.substring(pos_to_insert);
            sql = sql.substring(0, pos_to_insert);
        }
        if(!sql.toLowerCase().contains("where")) {
            sql += " where ";
        }
        else if(state.getRandomly().getBoolean()) {
            sql += " and ";
        }else{
            sql += " or ";
        }
        int leftBound = (int)state.getRandomly().getNotCachedInteger(-10000, 100000);
        int rightBound = (int)state.getRandomly().getNotCachedInteger(leftBound, 100000000);
        sql += col  + " >= " + Integer.toString(leftBound);
        if(state.getRandomly().getBoolean()) {
            sql += " and ";
        }else{
            sql += " or ";
        }
        sql += col + " <= " + Integer.toString(rightBound);
        sql +=  " " + suffix;
        return sql;

    }
}
