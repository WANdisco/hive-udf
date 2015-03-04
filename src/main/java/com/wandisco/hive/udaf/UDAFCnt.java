package com.wandisco.hive.udaf;

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
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;

import gnu.trove.set.hash.THashSet;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;

@Description(name = "count_distinct_long", value = "_FUNC_(x) - Distinct count for long values", extended = "Example:"
		+ "\n> SELECT count_distinct(values) FROM src")
public class UDAFCnt extends AbstractGenericUDAFResolver { // implements
															// GenericUDAFResolver2
															// {

	static final Log LOG = LogFactory.getLog(UDAFCnt.class.getName());

	@Override
	public GenericUDAFEvaluator getEvaluator(GenericUDAFParameterInfo info)
			throws SemanticException {
		TypeInfo[] parameters = info.getParameters();
		if (!(parameters[0].getCategory() == ObjectInspector.Category.PRIMITIVE)) {
			throw new UDFArgumentTypeException(0,
					"Only primitive type argument is accepted but "
							+ parameters[0].getTypeName()
							+ " was passed as parameter");
		}

		if (parameters.length > 1)
			throw new IllegalArgumentException(
					"Function only takes 1 parameter.");

		return new CountEvaluator();
	}

	@Override
	public GenericUDAFEvaluator getEvaluator(TypeInfo[] typeInfos)
			throws SemanticException {
		return new CountEvaluator();
	}

	public static class CountEvaluator extends GenericUDAFEvaluator {
		private Object[] partialResult;

		// inputs
		PrimitiveObjectInspector inputPrimitiveOI;
		StructObjectInspector inputStructOI;
		PrimitiveObjectInspector typeOI;
		PrimitiveObjectInspector paramOI;

		// intermediate results
		StandardListObjectInspector partialOI;

		public ObjectInspector init(Mode m, ObjectInspector[] parameters)
				throws HiveException {
			super.init(m, parameters);
			partialResult = new Object[2];
			partialResult[0] = new LongWritable(0);
			partialResult[1] = new DoubleWritable(0);
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
			reset(ceb);
			return ceb;
		}

		@Override
		public void reset(AggregationBuffer aggregationBuffer)
				throws HiveException {
			((CntAggregationBuffer) aggregationBuffer).hash = new THashSet();
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
			ceb.hash.add(x);
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
			THashSet hh = null;
			try {
				List<BytesWritable> partialResult = (List<BytesWritable>) partialOI
						.getList(partial);
				BytesWritable partialBytes = partialResult.get(0);
				ByteArrayInputStream bais = new ByteArrayInputStream(
						partialBytes.getBytes());
				ObjectInputStream oi = new ObjectInputStream(bais);
				hh = (THashSet) oi.readObject();
			} catch (Exception e) {
				throw new HiveException(e.getMessage());
			}
			mergeHashSets(hh, ceb);
		}

		private void mergeHashSets(THashSet hh, CntAggregationBuffer ceb) {
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
																				// {
			THashSet hash = new THashSet();

		}
	}
}