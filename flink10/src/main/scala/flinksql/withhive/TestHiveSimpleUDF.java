package flinksql.withhive;

import org.apache.hadoop.hive.ql.exec.MapredContext;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.util.Collections;

import static org.apache.flink.calcite.shaded.com.google.common.base.Preconditions.checkArgument;

/**
 * @author lj
 * @createDate 2019/12/30 17:17
 **/
public class TestHiveSimpleUDF extends UDF {

    public IntWritable evaluate(IntWritable i) {
        return new IntWritable(i.get());
    }

    public Text evaluate(Text text) {
        return new Text(text.toString());
    }
}

/**
 * Test generic udf. Registered under name 'mygenericudf'
 */
class TestHiveGenericUDF extends GenericUDF {

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        checkArgument(arguments.length == 2);

        checkArgument(arguments[1] instanceof ConstantObjectInspector);
        Object constant = ((ConstantObjectInspector) arguments[1]).getWritableConstantValue();
        checkArgument(constant instanceof IntWritable);
        checkArgument(((IntWritable) constant).get() == 1);

        if (arguments[0] instanceof IntObjectInspector ||
                arguments[0] instanceof StringObjectInspector) {
            return arguments[0];
        } else {
            throw new RuntimeException("Not support argument: " + arguments[0]);
        }
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        return arguments[0].get();
    }

    @Override
    public String getDisplayString(String[] children) {
        return "TestHiveGenericUDF";
    }
}

/**
 * Test split udtf. Registered under name 'mygenericudtf'
 */
class TestHiveUDTF extends GenericUDTF {


    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
        checkArgument(argOIs.length == 2);

        // TEST for constant arguments
        checkArgument(argOIs[1] instanceof ConstantObjectInspector);
        Object constant = ((ConstantObjectInspector) argOIs[1]).getWritableConstantValue();
        checkArgument(constant instanceof IntWritable);
        checkArgument(((IntWritable) constant).get() == 1);

        return ObjectInspectorFactory.getStandardStructObjectInspector(
                Collections.singletonList("col1"),
                Collections.singletonList(PrimitiveObjectInspectorFactory.javaStringObjectInspector));
    }

    @Override
    public void process(Object[] args) throws HiveException {
        String str = (String) args[0];
        for (String s : str.split(",")) {
            forward(s);
            forward(s);
        }
    }

    @Override
    public void close() {
    }
}
