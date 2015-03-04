package com.wandisco.hive.udaf;

import gnu.trove.set.hash.TLongHashSet;
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
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

import java.io.*;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

@Description(name = "count_distinct_bitset", value = "_FUNC_(x) - Distinct count for long values", extended = "Example:"
		+ "\n> SELECT count_distinct_bitset(values) FROM src")
public class UDAFCntBitSet extends AbstractGenericUDAFResolver { // implements
																	// GenericUDAFResolver2
																	// {

	static final Log LOG = LogFactory.getLog(UDAFCntBitSet.class.getName());

	// public static final int MAX_VALUE = 100000000;
	// public static final int NUM_LONGS = MAX_VALUE >> 6;

	@Override
	public GenericUDAFEvaluator getEvaluator(GenericUDAFParameterInfo info)
			throws SemanticException {
		TypeInfo[] parameters = info.getParameters();

		if (!parameters[0].getTypeName().equals("bigint")) {
			throw new SemanticException(
					"count_distinct_bitset UDAF only accepts bigint as first parameter");
		}

		if ((parameters.length > 1)
				&& !parameters[1].getTypeName().equals("bigint")) {
			throw new SemanticException("Base could only be bigint; Got "
					+ parameters[1].getTypeName());
		}

		if ((parameters.length == 3)
				&& !parameters[2].getTypeName().equals("int")) {
			throw new SemanticException("Size could only be int; Got "
					+ parameters[2].getTypeName());
		}

		return new CountEvaluator();
	}

	@Override
	public GenericUDAFEvaluator getEvaluator(TypeInfo[] typeInfos)
			throws SemanticException {
		return new CountEvaluator();
	}

	public static class CountEvaluator extends GenericUDAFEvaluator {
		// private Object[] partialResult;

		// inputs
		PrimitiveObjectInspector inputPrimitiveOI;

		// intermediate results
		StandardListObjectInspector partialOI;

		private long baseValue = 0;
		private int baseSize = 0;

		public ObjectInspector init(Mode m, ObjectInspector[] parameters)
				throws HiveException {
			super.init(m, parameters);

			if (parameters.length > 1) {
				if (!(parameters[1] instanceof ConstantObjectInspector)) {
					throw new HiveException("Base Value must be a constant");
				}
				ConstantObjectInspector baseOI = (ConstantObjectInspector) parameters[1];
				this.baseValue = ((LongWritable) baseOI
						.getWritableConstantValue()).get();

				if (parameters.length == 3) {
					ConstantObjectInspector sizeOI = (ConstantObjectInspector) parameters[2];
					this.baseSize = ((IntWritable) sizeOI
							.getWritableConstantValue()).get();
				} else {
					this.baseSize = 0;
				}
			} else {
				this.baseValue = 0;
				this.baseSize = 0;
			}

			if (m == Mode.PARTIAL1 || m == Mode.COMPLETE) {
				assert (parameters.length == 1);
				ObjectInspector.Category cat = parameters[0].getCategory();
				switch (cat) {
				case PRIMITIVE:
					inputPrimitiveOI = (PrimitiveObjectInspector) parameters[0];
					break;
				default:
					throw new IllegalArgumentException(
							"Only PRIMITIVE types are allowed as input. Passed a "
									+ cat.name());
				}
			} else {
				// partial input object inspector for intermediate results
				partialOI = (StandardListObjectInspector) parameters[0];
			}
			if (m == Mode.PARTIAL1 || m == Mode.PARTIAL2) {
				return ObjectInspectorFactory
						.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableBinaryObjectInspector);
			} else {
				return PrimitiveObjectInspectorFactory.writableLongObjectInspector;
			}
		}

		@Override
		public AggregationBuffer getNewAggregationBuffer() throws HiveException {
			CntAggregationBuffer ceb = new CntAggregationBuffer();
			ceb.init(baseSize);
			reset(ceb);
			return ceb;
		}

		@Override
		public void reset(AggregationBuffer aggregationBuffer)
				throws HiveException {
			((CntAggregationBuffer) aggregationBuffer).set.clear();
		}

		@Override
		public void iterate(AggregationBuffer aggregationBuffer,
				Object[] parameters) throws HiveException {
			if (parameters[0] == null) {
				return;
			}
			CntAggregationBuffer ceb = (CntAggregationBuffer) aggregationBuffer;
			Object x = ObjectInspectorUtils.copyToStandardObject(parameters[0],
					inputPrimitiveOI, ObjectInspectorCopyOption.JAVA);

			long value = Math.abs((Long) x - baseValue);
			if (value > 0 && value < Integer.MAX_VALUE)
				ceb.set.set((int) value);
		}

		@Override
		public Object terminatePartial(AggregationBuffer aggregationBuffer)
				throws HiveException {
			CntAggregationBuffer ceb = (CntAggregationBuffer) aggregationBuffer;
			ByteArrayOutputStream b = new ByteArrayOutputStream();
			try {
				ObjectOutputStream o = new ObjectOutputStream(b);
				o.writeObject(ceb.set);
			} catch (IOException e) {
				throw new HiveException(e.getMessage());
			}
			byte[] arr = b.toByteArray();
			List<BytesWritable> bl = new ArrayList<BytesWritable>();
			bl.add(new BytesWritable(arr));
			return bl;
		}

		@Override
		public void merge(AggregationBuffer aggregationBuffer, Object partial)
				throws HiveException {
			if (partial == null) {
				return;
			}
			CntAggregationBuffer ceb = (CntAggregationBuffer) aggregationBuffer;
			BitSet mSet = null;

			try {
				List<BytesWritable> partialResult = (List<BytesWritable>) partialOI
						.getList(partial);
				BytesWritable partialBytes = partialResult.get(0);
				ByteArrayInputStream bais = new ByteArrayInputStream(
						partialBytes.getBytes());
				ObjectInputStream oi = new ObjectInputStream(bais);
				mSet = (BitSet) oi.readObject();
			} catch (Exception e) {
				throw new HiveException(e.getMessage());
			}
			ceb.set.or(mSet);
		}

		@Override
		public Object terminate(AggregationBuffer aggregationBuffer)
				throws HiveException {
			CntAggregationBuffer ceb = (CntAggregationBuffer) aggregationBuffer;
			if (ceb.set == null) {
				return null;
			}
			return new LongWritable(ceb.set.cardinality());
		}

		static class CntAggregationBuffer extends AbstractAggregationBuffer { // implements
																				// AggregationBuffer
																				// {
			// TLongHashSet hash = new TLongHashSet();

			BitSet set = null;

			void init(int size) {
				if (size == 0) {
					set = new BitSet();
				} else {
					set = new BitSet(size);
				}
			}

			/*
			 * long [] words = new long[NUM_LONGS];
			 * 
			 * 
			 * 
			 * void setBit(int bit) throws SemanticException { if(bit >
			 * MAX_VALUE) throw new
			 * SemanticException("Value exceeded max value = " + bit); int widx
			 * = bit >> 6; words[widx] |= (1L << bit); }
			 * 
			 * boolean getBit(int bit) throws SemanticException { int widx = bit
			 * >> 6; return (widx < MAX_VALUE) && ((words[widx] & (1L << bit))
			 * != 0); }
			 * 
			 * void mergeSets(long[] mWords) { for(int i=0; i< NUM_LONGS; ++i) {
			 * words[i] |= mWords[i]; } }
			 * 
			 * int cardinality() { int sum = 0; for(int i=0; i< NUM_LONGS; ++i)
			 * { sum += Long.bitCount(words[i]); } return sum; }
			 * 
			 * void clear() { for(int i=0; i < NUM_LONGS; ++i) { words[i] = 0; }
			 * }
			 */
		}
	}
}