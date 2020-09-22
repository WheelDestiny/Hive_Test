package com.wheelDestiny.hive.function;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFParameterInfo;
import org.apache.hadoop.hive.serde2.lazy.LazyInteger;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryStruct;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;

import java.util.ArrayList;
import java.util.List;

public class AvgUDAF extends AbstractGenericUDAFResolver {

    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] info) throws SemanticException {
        //校验参数个数
        if(info.length!=1){
            throw new SemanticException("input param must one");
        }
        //校验参数大类
        if(!info[0].getCategory().equals(ObjectInspector.Category.PRIMITIVE)){
            throw new SemanticException("input param must PRIMITIVE");
        }
        //校验参数具体类型
        if(!info[0].getTypeName().equalsIgnoreCase(PrimitiveObjectInspector.PrimitiveCategory.INT.name())){
            throw new SemanticException("input param must INT");
        }
        return new AvgEvaluator();
    }

    public static class AvgEvaluator extends GenericUDAFEvaluator{

        public static class AvgAggBean extends AbstractAggregationBuffer{
            //用来存放中间结果
            private int sum = 0;
            private int count = 0;

            public int getCount() {
                return count;
            }

            public void setCount(int count) {
                this.count = count;
            }

            public int getSum() {
                return sum;
            }

            public void setSum(int sum) {
                this.sum = sum;
            }
        }
        //部分输出
        //第一个元素：sum值
        //第二个元素：count值
        Object[] partialOut = {new IntWritable(),new IntWritable()};
        //整体输出
        DoubleWritable out  = new DoubleWritable();

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            //初始化对象
            return new AvgAggBean();
        }
        @Override
        public void reset(AggregationBuffer aggregationBuffer) throws HiveException {
            //重置Bean对象中的数据
            AvgAggBean avgAggBean = (AvgAggBean)aggregationBuffer;
            avgAggBean.setSum(0);
            avgAggBean.setCount(0);
        }

        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            //init方法中一定切记要调用父类的init初始化，用于自动创建各种GenericUDAFEvaluator类中需要的对象
            super.init(m, parameters);
            //设置每个阶段的输出
            //map阶段需要输出复合struct结构
            if(m == Mode.PARTIAL1 || m == Mode.PARTIAL2){
                //struct内部字段名称列表
                List<String> names = new ArrayList<>();
                names.add("sum");
                names.add("count");
                //struct内部字段名称对应的类型列表
                List<ObjectInspector> inspectors = new ArrayList<>();
                inspectors.add(PrimitiveObjectInspectorFactory.writableIntObjectInspector);
                inspectors.add(PrimitiveObjectInspectorFactory.writableIntObjectInspector);
                return ObjectInspectorFactory.getStandardStructObjectInspector(names,inspectors);
            }

            //reduce阶段的最终输出
            return PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
        }
        @Override
        public void iterate(AggregationBuffer aggregationBuffer, Object[] objects) throws HiveException {
            //聚合操作的函数
            //把一个mapper的每行数据做统计放入中间的bean对象中
            Object o = objects[0];
            IntWritable intWritable = null;
            if(o instanceof LazyInteger){
                LazyInteger lzI = (LazyInteger)o;
                intWritable = lzI.getWritableObject();
            }else if(o instanceof IntWritable){
                intWritable = (IntWritable)o;
            }

            //把每行统计的局部结果封装到bean中
            int currNum = intWritable.get();
            AvgAggBean avgAggBean =  (AvgAggBean)aggregationBuffer;
            int oleNum = avgAggBean.getSum();
            //累计求和
            avgAggBean.setSum(oleNum+currNum);
            //计算总记录数
            avgAggBean.setCount(avgAggBean.getCount()+1);
        }
        @Override
        public Object terminatePartial(AggregationBuffer aggregationBuffer) throws HiveException {
            //局部结果转化为需要输出的结果类型
            AvgAggBean sumAggBean =  (AvgAggBean)aggregationBuffer;
            ((IntWritable)partialOut[0]).set(sumAggBean.getSum());
            ((IntWritable)partialOut[1]).set(sumAggBean.getCount());
            return partialOut;
        }

        @Override
        public void merge(AggregationBuffer aggregationBuffer, Object o) throws HiveException {
            //每个map的结果进行合并
            IntWritable sumParam = null;
            IntWritable countParam = null;
            if(o instanceof LazyBinaryStruct){
                LazyBinaryStruct lzS = (LazyBinaryStruct)o;
                //获取sum值
                sumParam = (IntWritable)lzS.getField(0);
                //获取count值
                countParam = (IntWritable)lzS.getField(1);
            }

            int currentSum = sumParam.get();
            int currentCount = countParam.get();

            AvgAggBean avgAggBean = (AvgAggBean)aggregationBuffer;
            avgAggBean.setSum(avgAggBean.getSum()+currentSum);
            avgAggBean.setCount(avgAggBean.getCount()+currentCount);
        }

        @Override
        public Object terminate(AggregationBuffer aggregationBuffer) throws HiveException {
            //将最终结果转化为需要的输出类型
            AvgAggBean avgAggBean = (AvgAggBean)aggregationBuffer;
            double avg = (double)avgAggBean.getSum()/avgAggBean.getCount();
            out.set(avg);
            return out;
        }
    }

}
