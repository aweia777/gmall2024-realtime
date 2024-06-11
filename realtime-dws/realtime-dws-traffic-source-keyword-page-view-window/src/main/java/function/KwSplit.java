package function;

import com.atguigu.gmall.realtime.common.util.IkUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.util.List;


@FunctionHint(output = @DataTypeHint("ROW<keyword STRING,length INT>"))
public class KwSplit extends TableFunction {
    public void eval(String keywords) {
        List<String> strings = IkUtil.IkSplit(keywords);
        for (String string : strings) {
            collect(Row.of(string,string.length()));
        }
    }
}
