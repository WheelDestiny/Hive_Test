package com.wheelDestiny.hive.function;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.lazy.LazyString;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

/**
 * 自定义UDF函数，实现国家码转国家名称
 */
public class Country2CountryName extends GenericUDF {

    //存储国家码国家名称的映射关系
    private static Map<String,String> map = new HashMap<String, String>();

    static {
        try (
                //将需要关闭的流写在try()中，自动关闭流
            InputStream inputStream = Country2CountryName.class.getResourceAsStream("/country.dat");
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, "utf-8"));
            )
        {
            String line = null;
            while ((line =reader.readLine())!=null ){
                String[] split = line.split("\t");
                String code = split[0];
                String name = split[1];
                map.put(code,name);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    /**
     * 初始化
     * @param objectInspectors
     * @return
     * @throws UDFArgumentException
     */
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        //校验参数是否合法，设定返回值类型
        //校验参数个数 1
        if(objectInspectors.length!=1){
            throw new UDFArgumentException("input param must one!");
        }
        //校验参数类型(大类，PRIMITIVE, LIST, MAP, STRUCT, UNION)
        ObjectInspector inspector = objectInspectors[0];
        if(!inspector.getCategory().equals(ObjectInspector.Category.PRIMITIVE)){
            throw new UDFArgumentException("input param type must primitive!");
        }
        //校验参数具体类型，类型必须为String
        if(!inspector.getTypeName().equalsIgnoreCase(PrimitiveObjectInspector.PrimitiveCategory.STRING.name())){
            throw new UDFArgumentException("input param type must String!");
        }
        //设定返回值类型(Text)
        return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    }

    //函数要返回的对象
    private Text output = new Text();
    /**
     * 核心算法，每条数据执行一次
     * @param deferredObjects
     * @return
     * @throws HiveException
     */
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        //拿到输入参数
        Object input = deferredObjects[0].get();

        Text inputParam = null;
        //LazyString是hive自带的类型，封装的是Text类型
        if(input instanceof LazyString){
            LazyString lazyString = (LazyString) input;
            inputParam = lazyString.getWritableObject();
        }else if(input instanceof Text){
            inputParam = (Text) input;
        }
        String countryCode = inputParam.toString();
        //根据map找到匹配的国家名称
        String countryName = map.get(countryCode);
        countryName = countryName==null?"其他":countryName;
        output.set(countryName);
        //输出相对应的国家名称
        return output;
    }

    /**
     * 错误输出
     * @param strings
     * @return
     */
    public String getDisplayString(String[] strings) {
        return "Transform error";
    }


}
