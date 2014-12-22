package com.wandisco.hive.udaf;

import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import com.clearspring.analytics.stream.cardinality.ICardinality;
import com.clearspring.analytics.stream.cardinality.LinearCounting;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFParameterInfo;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFResolver2;
import org.apache.hadoop.hive.serde2.lazy.LazyBinary;
import org.apache.hadoop.hive.serde2.lazy.LazyString;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

/**
 * This code is based on the mlnick implementation of HLL and LC approximate counters
 * from https://githib.com/mlnick/hive-udf
 * Changes made to improve performance and use improved HyperLogLog++ algorithm.
 */

@Description(name = "approx_distinct_hll",
        value = "_FUNC_(x) - Adds values from x to new HyperLogLogPlus Cardinality Estimator " +
                "\nReturns a long cardinality",
        extended =  "Example:" +
                "\n> SELECT approx_distinct(values) FROM src; -- calls HyperLogLogPlus with b=16")
public class UDAFCntHLL extends AbstractGenericUDAFResolver { //implements GenericUDAFResolver2 {
    static final Log LOG = LogFactory.getLog(UDAFCntHLL.class.getName());
    public static final String BINARY = "binary";

    @Override
    public GenericUDAFEvaluator getEvaluator(GenericUDAFParameterInfo info) throws SemanticException {
        TypeInfo[] parameters = info.getParameters();
        if (!(parameters[0].getCategory() == ObjectInspector.Category.PRIMITIVE ||
                parameters[0].getCategory() == ObjectInspector.Category.STRUCT))  {
            throw new UDFArgumentTypeException(0,
                    "Only primitive type or struct arguments are accepted but "
                            + parameters[0].getTypeName() + " was passed as parameter 1.");
        }
        if (parameters.length == 2) {
            if (parameters[1].getCategory() != ObjectInspector.Category.PRIMITIVE) {
                throw new UDFArgumentTypeException(1,
                        "Only primitive type arguments are accepted but "
                                + parameters[1].getTypeName() + " was passed as parameter 2.");
            }
            if( ((PrimitiveTypeInfo) parameters[1]).getPrimitiveCategory()
                    != PrimitiveObjectInspector.PrimitiveCategory.INT) {
                throw new UDFArgumentTypeException(1,
                        "Only a int argument is accepted as parameter 2, but "
                                + parameters[1].getTypeName() + " was passed instead.");
            }
        }

        if (parameters.length > 2) throw new IllegalArgumentException("Function only takes 1 or 2 parameters.");

        return new CardinalityEstimatorEvaluator();
    }

    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] typeInfos) throws SemanticException {
        return new CardinalityEstimatorEvaluator();
    }

    /**
     * Class to evaluate values, and add them to an approximate cardinality estimator datastructure.
     * Currently HyperLogLog and Linear Counting are supported.
     */
    public static class CardinalityEstimatorEvaluator extends GenericUDAFEvaluator {
        PrimitiveObjectInspector inputPrimitiveOI;
        StructObjectInspector inputStructOI;
        PrimitiveObjectInspector paramOI;

        // intermediate results
        StandardListObjectInspector partialOI;

        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            super.init(m, parameters);

            // init input object inspectors
            if (m == Mode.PARTIAL1 || m == Mode.COMPLETE) {
                assert (parameters.length >= 1 && parameters.length <= 3);
                ObjectInspector.Category cat = parameters[0].getCategory();
                switch (cat) {
                    case PRIMITIVE:
                        inputPrimitiveOI = (PrimitiveObjectInspector) parameters[0];
                        switch (parameters.length) {
                            case 2:
                                paramOI = (PrimitiveObjectInspector) parameters[1];
                                break;
                        }
                        break;
                    case STRUCT:
                        inputStructOI = (StructObjectInspector) parameters[0];
                        break;
                    default:
                        throw new IllegalArgumentException("Only PRIMITIVE types and existing STRUCTs " +
                                "containing cardinality estimators are allowed as input. Passed a " + cat.name());
                }
            }
            else {
                // partial input object inspector for intermediate results
                partialOI = (StandardListObjectInspector) parameters[0];
            }

            // output object inspectors
            if (m == Mode.PARTIAL1 || m == Mode.PARTIAL2) {
                return ObjectInspectorFactory.getStandardListObjectInspector(
                        PrimitiveObjectInspectorFactory.writableBinaryObjectInspector);
            }
            else {
              return PrimitiveObjectInspectorFactory.writableLongObjectInspector;
            }
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            CardinalityEstimatorBuffer ceb = new CardinalityEstimatorBuffer();
            reset(ceb);
            return ceb;
        }

        @Override
        public void reset(AggregationBuffer aggregationBuffer) throws HiveException {
            ((CardinalityEstimatorBuffer) aggregationBuffer).cardinalityEstimator = null;
        }

        @Override
        public void iterate(AggregationBuffer aggregationBuffer, Object[] parameters) throws HiveException {
            if (parameters[0] == null) {
                return;
            }
/*
            int param = -1;
            if (parameters.length == 2) {
               param = PrimitiveObjectInspectorUtils.getInt(parameters[1], paramOI);
            }
*/
            CardinalityEstimatorBuffer ceb = (CardinalityEstimatorBuffer) aggregationBuffer;

            Object obj = parameters[0];
            if (inputPrimitiveOI != null) {
                // in this case, we process the object directly
                if (ceb.cardinalityEstimator == null) {
//                  ceb.cardinalityEstimator = new HyperLogLogPlus(param < 0 ? HLL_DEFAULT_B : param);
                  ceb.cardinalityEstimator = new HyperLogLogPlus(16);
                }
                ceb.cardinalityEstimator.offer(obj);
            }
            else if (inputStructOI != null) {
                // in this case we merge estimators
                //LazyString type = (LazyString) inputStructOI.getStructFieldData(obj, inputStructOI.getStructFieldRef(ESTIMATOR_TYPE));
              try {
                LazyBinary lb = (LazyBinary) inputStructOI.getStructFieldData(obj, inputStructOI.getStructFieldRef(BINARY));
                ICardinality that = HyperLogLogPlus.Builder.build(lb.getWritableObject().getBytes());
                mergeEstimators(that, ceb);
              } catch(IOException e) {
                throw new HiveException("Failed to parse byte[] from partial result. ", e);
              }
            }
        }

      byte[] tb = {1};
        /**
         * Return partial result of aggregation.
         * @param aggregationBuffer current state of the aggregation
         * @return partial result, in the form of a list of {@link BytesWritable} containing the estimator type, and the
         * serialised estimator
         * @throws HiveException
         */
        @Override
        public Object terminatePartial(AggregationBuffer aggregationBuffer) throws HiveException {
            CardinalityEstimatorBuffer ceb = (CardinalityEstimatorBuffer) aggregationBuffer;

            try {
                byte[] ceBytes = ceb.cardinalityEstimator.getBytes();
                List<BytesWritable> b = new ArrayList<BytesWritable>();
                b.add(new BytesWritable(tb));
                b.add(new BytesWritable(ceBytes));
                return b;
            } catch (IOException e) {
                throw new HiveException("Failed to extract byte[] during partial termination. ", e);
            }
        }

        /**
         * Merges a partial estimator
         * @param aggregationBuffer current state of the aggregation
         * @param partial estimator object to be merged (see {@link #terminatePartial(org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AggregationBuffer)}
         * @throws HiveException
         */
        @Override
        public void merge(AggregationBuffer aggregationBuffer, Object partial) throws HiveException {
            if (partial == null) {
                return;
            }
            CardinalityEstimatorBuffer ceb = (CardinalityEstimatorBuffer) aggregationBuffer;

            List<BytesWritable> partialResult = (List<BytesWritable>) partialOI.getList(partial);
            assert (partialResult.size() == 2);
            BytesWritable partialString = partialResult.get(0);
            String type = null;
            try {
                type = new String(partialString.getBytes(), "UTF-8");
            } catch (UnsupportedEncodingException e) {
                throw new HiveException(e);
            }
            BytesWritable partialBytes = partialResult.get(1);

            // Parse the serialised partial result and merge
          try {
            ICardinality partialEstimator = HyperLogLogPlus.Builder.build(partialBytes.getBytes());
            mergeEstimators(partialEstimator, ceb);
          } catch(IOException e) {
            throw new HiveException("Failed to extract byte[] during partial termination: " + e.getMessage(), e);
          }
        }

        /**
         * Return the final state of the aggregation
         * @param aggregationBuffer current state of the aggregation
         * @return Hive struct data type, {type: string, cardinality: bigint, binary: binary}, containing the type of the
         * estimator, the estimated cardinality, and the serialised estimator
         * @throws HiveException
         */
        @Override
        public Object terminate(AggregationBuffer aggregationBuffer) throws HiveException {
            CardinalityEstimatorBuffer ceb = (CardinalityEstimatorBuffer) aggregationBuffer;
            if (ceb.cardinalityEstimator == null) {
                return null;
            }
            try {
                ArrayList<Object> result = new ArrayList<Object>();
                result.add(new Text("HLL"));
                long cardinality = ceb.cardinalityEstimator instanceof HyperLogLogPlus
                        ? ((HyperLogLogPlus) ceb.cardinalityEstimator).cardinality()
                        : ceb.cardinalityEstimator.cardinality();
              return new LongWritable(cardinality);
//                result.add(new LongWritable(cardinality));
//                result.add(new BytesWritable(ceb.cardinalityEstimator.getBytes()));
//                return result;
            } catch (Exception e) {
                throw new HiveException("Failed to extract serialised Cardinality Estimator instance. ", e);
            }
        }

        // HELPER METHODS //


        /**
         * Either merge a partial estimator into the current aggregation buffer, or if the buffer is empty,
         * simply set to the partial estimator
         * @param thatEstimator the cardinality estimator to merge in
         * @param thisEstimatorBuffer the current aggregation buffer instance
         * @throws HiveException
         */
        private static void mergeEstimators(ICardinality thatEstimator, CardinalityEstimatorBuffer thisEstimatorBuffer) throws HiveException {
            try {
                if (thisEstimatorBuffer.cardinalityEstimator == null) {
                    thisEstimatorBuffer.cardinalityEstimator = thatEstimator;
                    LOG.debug("Aggregation buffer is null, using THAT partial instance. Cardinality result = " + thisEstimatorBuffer.cardinalityEstimator.cardinality());
                }
                else {
                    //LOG.debug("Merging estimator instances, with THIS partial result = " + thisEstimatorBuffer.cardinalityEstimator.cardinality() +
                    //        " and THAT partial result = " + thatEstimator.cardinality());
                    thisEstimatorBuffer.cardinalityEstimator = thisEstimatorBuffer.cardinalityEstimator.merge(thatEstimator);
                    //LOG.debug("MERGED partial result = " + thisEstimatorBuffer.cardinalityEstimator.cardinality());
                }
            } catch (CardinalityMergeException e) {
                throw new HiveException("Failed to merge Cardinality Estimator instances due to cardinality error. ", e);
            }
        }

        /**
         * Wrapper for {@link ICardinality} instance to which values are added
         */
        static class CardinalityEstimatorBuffer extends AbstractAggregationBuffer {  //implements AggregationBuffer {
            ICardinality cardinalityEstimator;
        }
    }
}
