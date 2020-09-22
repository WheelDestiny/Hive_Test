package com.wheelDestiny.hive.function;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.lazy.LazyString;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;

import java.util.ArrayList;
import java.util.List;

public class SplitUDTF extends GenericUDTF {
    //输出复合struct
    //第一个元素是name，第二个元素是nickname
    Object[] out = {new Text(),new Text()};

    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {

        //校验参数是否合法，设定返回值类型
        //校验参数个数 1
        if(argOIs.length!=1){
            throw new UDFArgumentException("input param must one!");
        }
        //校验参数类型(大类，PRIMITIVE, LIST, MAP, STRUCT, UNION)
        ObjectInspector inspector = argOIs[0];
        if(!inspector.getCategory().equals(ObjectInspector.Category.PRIMITIVE)){
            throw new UDFArgumentException("input param type must primitive!");
        }
        //校验参数具体类型，类型必须为String
        if(!inspector.getTypeName().equalsIgnoreCase(PrimitiveObjectInspector.PrimitiveCategory.STRING.name())){
            throw new UDFArgumentException("input param type must String!");
        }
        //struct 内部字典名称列表
        List<String> names = new ArrayList<>();
        names.add("name");
        names.add("nickname");
        //struct内部字段名称对应的类型列表
        List<ObjectInspector> inspectors = new ArrayList<>();
        inspectors.add(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
        inspectors.add(PrimitiveObjectInspectorFactory.writableStringObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(names,inspectors);
    }

    @Override
    public void process(Object[] args) throws HiveException {
        //处理UDTF函数逻辑，通过调用forword输出一行数据
        Object obj = args[0];
        Text inputParam = null;

        if(obj instanceof LazyString){
            LazyString lz = (LazyString)obj;
            inputParam = lz.getWritableObject();
        }else if(obj instanceof Text){
            inputParam = (Text)obj;
        }

        String line = inputParam.toString();

        String[] arr = line.split(";");
        for (String s : arr) {
            String[] split = s.split("#");
            String name = split[0];
            String nickname = split[1];
            ((Text)out[0]).set(name);
            ((Text)out[1]).set(nickname);
            //调用一次forword输出一行数据
            forward(out);
        }

    }



    @Override
    public void close() throws HiveException {

    }
}
