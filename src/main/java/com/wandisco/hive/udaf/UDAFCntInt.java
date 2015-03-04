package com.wandisco.hive.udaf;

import gnu.trove.set.hash.TIntHashSet;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFParameterInfo;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

@Description(name = "count_distinct_int", value = "_FUNC_(x) - Distinct count for long values", extended = "Example:"
		+ "\n> SELECT count_distinct_int(values) FROM src")
public class UDAFCntInt extends AbstractGenericUDAFResolver { // implementsGenericUDAFResolver2

	static final Log LOG = LogFactory.getLog(UDAFCntInt.class.getName());

	@Override
	public GenericUDAFEvaluator getEvaluator(GenericUDAFParameterInfo info)
			throws SemanticException {
		TypeInfo[] parameters = info.getParameters();

		if (!parameters[0].getTypeName().equals("bigint")
				&& !parameters[0].getTypeName().equals("int")) {
			throw new SemanticException(
					"count_distinct_int UDAF only accepts int or bigint as first parameter");
		}

		if ((parameters.length > 1)
				&& !parameters[1].getTypeName().equals("bigint")
				&& !parameters[1].getTypeName().equals("int")) {
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
			// reset(ceb);
			return ceb;
		}

		@Override
		public void reset(AggregationBuffer aggregationBuffer)
				throws HiveException {
			((CntAggregationBuffer) aggregationBuffer).hash.clear();
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
			ceb.hash.add((int) value);
		}

		@Override
		public Object terminatePartial(AggregationBuffer aggregationBuffer)
				throws HiveException {
			CntAggregationBuffer ceb = (CntAggregationBuffer) aggregationBuffer;
			ByteArrayOutputStream b = new ByteArrayOutputStream();
			try {
				ObjectOutputStream o = new ObjectOutputStream(b);
				o.writeObject(ceb.hash);
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
			TIntHashSet hh = null;

			try {
				List<BytesWritable> partialResult = (List<BytesWritable>) partialOI
						.getList(partial);
				BytesWritable partialBytes = partialResult.get(0);
				ByteArrayInputStream bais = new ByteArrayInputStream(
						partialBytes.getBytes());
				ObjectInputStream oi = new ObjectInputStream(bais);
				hh = (TIntHashSet) oi.readObject();
			} catch (Exception e) {
				throw new HiveException(e.getMessage());
			}
			mergeHashSets(hh, ceb);
		}

		private void mergeHashSets(TIntHashSet hh, CntAggregationBuffer ceb) {
			if (ceb.hash.size() == 0) {
				ceb.hash = hh;
				return;
			}
			if (ceb.hash.size() > hh.size()) {
				ceb.hash.addAll(hh);
			} else {
				hh.addAll(ceb.hash);
				ceb.hash = hh;
			}
		}

		@Override
		public Object terminate(AggregationBuffer aggregationBuffer)
				throws HiveException {
			CntAggregationBuffer ceb = (CntAggregationBuffer) aggregationBuffer;
			if (ceb.hash == null) {
				return null;
			}
			return new LongWritable(ceb.hash.size());
		}

		static class CntAggregationBuffer extends AbstractAggregationBuffer { // implements
																				// AggregationBuffer

			TIntHashSet hash = null;

			void init(int size) {
				if (size == 0) {
					hash = new TIntHashSet();
				} else {
					hash = new TIntHashSet(size);
				}
			}
		}
	}
}