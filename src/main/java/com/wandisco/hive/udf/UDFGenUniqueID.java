package com.wandisco.hive.udf;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.IntWritable;

/**
 * Created by jjb on 8/18/14.
 */
@Description(name = "gen_unique_id", value = "_FUNC_(str) - Returns a unique id of the string argument")
public class UDFGenUniqueID extends GenericUDF {
	private ObjectInspector[] argumentOIs;

	@Override
	public ObjectInspector initialize(ObjectInspector[] arguments)
			throws UDFArgumentTypeException {

		argumentOIs = arguments;
		return PrimitiveObjectInspectorFactory.writableIntObjectInspector;
	}

	private IntWritable result = new IntWritable();

	@Override
	public Object evaluate(GenericUDF.DeferredObject[] arguments)
			throws HiveException {
		// See
		// http://java.sun.com/j2se/1.5.0/docs/api/java/util/List.html#hashCode()
		int r = 0;
		for (int i = 0; i < arguments.length; i++) {
			r = r
					* 31
					+ ObjectInspectorUtils.hashCode(arguments[i].get(),
							argumentOIs[i]);
		}
		result.set(r);
		return result;
	}

	@Override
	public String getDisplayString(String[] children) {
		return "hash(" + StringUtils.join(children, ',') + ")";
	}

}
