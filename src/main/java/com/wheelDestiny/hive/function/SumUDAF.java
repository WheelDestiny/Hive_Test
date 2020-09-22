package com.wheelDestiny.hive.function;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFParameterInfo;
import org.apache.hadoop.hive.serde2.lazy.LazyInteger;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.IntWritable;

public class SumUDAF extends AbstractGenericUDAFResolver {

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
        return new SumEvaluator();
    }

    @Override
    public GenericUDAFEvaluator getEvaluator(GenericUDAFParameterInfo info) throws SemanticException {
        return super.getEvaluator(info);
    }
    public static class SumEvaluator extends GenericUDAFEvaluator{

        public static class SumAggBean extends AbstractAggregationBuffer{
            //用来存放中间结果
            private int sum = 0;

            public int getSum() {
                return sum;
            }

            public void setSum(int sum) {
                this.sum = sum;
            }
        }
        //map阶段的输出，局部聚合的结果
        IntWritable mapOutput = new IntWritable();

        //reduce阶段的输出，整体的输出结果
        IntWritable reduceOutput = new IntWritable();

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            System.out.println("getNewAggregationBuffer()");
            //初始化对象
            return new SumAggBean();
        }

        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            //设置每个阶段的输出
            //当前函数的业务逻辑中所有的输出都是int类型
            super.init(m,parameters);
            return PrimitiveObjectInspectorFactory.writableIntObjectInspector;


        }

        @Override
        public void reset(AggregationBuffer aggregationBuffer) throws HiveException {
            //重置Bean对象中的数据
            SumAggBean sumAggBean = (SumAggBean)aggregationBuffer;
            sumAggBean.setSum(0);
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

            //获取到之前的累加和，和当前的元素累加
            int currNum = intWritable.get();
            SumAggBean sumAggBean =  (SumAggBean)aggregationBuffer;
            int oleNum = sumAggBean.getSum();
            sumAggBean.setSum(oleNum+currNum);
        }

        @Override
        public Object terminatePartial(AggregationBuffer aggregationBuffer) throws HiveException {
            //局部结果转化为需要输出的结果类型
            SumAggBean sumAggBean =  (SumAggBean)aggregationBuffer;
            mapOutput.set(sumAggBean.getSum());
            return mapOutput;
        }

        @Override
        public void merge(AggregationBuffer aggregationBuffer, Object o) throws HiveException {
            //每个map的结果进行合并
            IntWritable intWritable = null;
            if(o instanceof LazyInteger){
                LazyInteger lzI = (LazyInteger)o;
                intWritable = lzI.getWritableObject();
            }else if(o instanceof IntWritable){
                intWritable = (IntWritable)o;
            }

            int currNum = intWritable.get();
            SumAggBean sumAggBean =  (SumAggBean)aggregationBuffer;
            int oleNum = sumAggBean.getSum();
            sumAggBean.setSum(oleNum+currNum);
        }

        @Override
        public Object terminate(AggregationBuffer aggregationBuffer) throws HiveException {
            //将最终结果转化为需要的输出类型
            SumAggBean sumAggBean =  (SumAggBean)aggregationBuffer;
            mapOutput.set(sumAggBean.getSum());
            return mapOutput;
        }
    }

}
