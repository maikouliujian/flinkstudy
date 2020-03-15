package flinksql.udtf;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

public class MyUDTF extends TableFunction<Row> {

    public void eval(String s){
        JSONArray jsonArray = JSONArray.parseArray(s);
        for(int i =0; i < jsonArray.size(); i++){
            JSONObject jsonObject = jsonArray.getJSONObject(i);
            String type = jsonObject.getString("type");
            String value = jsonObject.getString("value");
            collector.collect(Row.of(type, value));

        }

    }


    @Override
    public TypeInformation<Row> getResultType(){
//        return DataTypes.ROW(DataTypes.FIELD("type", DataTypes.STRING(),""),
//                DataTypes.FIELD("type", DataTypes.STRING(),""));
       return Types.ROW(Types.STRING(),Types.STRING());
    }

}
