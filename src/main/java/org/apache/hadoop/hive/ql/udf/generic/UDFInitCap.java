/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.udf.generic;

import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping.STRING_GROUP;

import org.apache.commons.lang.WordUtils;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedExpressions;
import org.apache.hadoop.hive.ql.exec.vector.expressions.ExtendedStringInitCap;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.io.Text;

/**
 * UDFInitCap.
 *
 */
@Description(
    name = "initcap",
    value = "_FUNC_(str) - Returns str, with the first letter of each word in uppercase,"
        + " all other letters in lowercase. Words are delimited by white space.",
    extended = "Example:\n" + " > SELECT _FUNC_('tHe soap') FROM src LIMIT 1;\n" + " 'The Soap'")
@VectorizedExpressions({ ExtendedStringInitCap.class })
public class UDFInitCap extends GenericUDF {
  private transient PrimitiveCategory[] inputTypes = new PrimitiveCategory[1];
  private transient Converter[] converters = new Converter[1];
  private final Text output = new Text();

  private static final String[] ORDINAL_SUFFIXES = new String[] { "th", "st", "nd", "rd", "th",
      "th", "th", "th", "th", "th" };

  protected String getStringValue(DeferredObject[] arguments, int i,            Converter[] converters)
      throws HiveException {
    Object obj;
    if ((obj = arguments[i].get()) == null) {
      return null;
    }
    return converters[i].convert(obj).toString();
  }

  protected void obtainStringConverter(ObjectInspector[] arguments, int i,
      PrimitiveCategory[] inputTypes, Converter[] converters) throws            UDFArgumentTypeException {
    PrimitiveObjectInspector inOi = (PrimitiveObjectInspector) arguments[i];
    PrimitiveCategory inputType = inOi.getPrimitiveCategory();

    Converter converter = ObjectInspectorConverters.getConverter(
        arguments[i],
        PrimitiveObjectInspectorFactory.writableStringObjectInspector);
    converters[i] = converter;
    inputTypes[i] = inputType;
  }

  protected String getStandardDisplayString(String name, String[] children) {
    return getStandardDisplayString(name, children, ", ");
  }

  protected String getStandardDisplayString(String name, String[] children,     String delim) {
    StringBuilder sb = new StringBuilder();
    sb.append(name);
    sb.append("(");
    if (children.length > 0) {
      sb.append(children[0]);
      for (int i = 1; i < children.length; i++) {
        sb.append(delim);
        sb.append(children[i]);
      }
    }
    sb.append(")");
    return sb.toString();
  }




  protected String getArgOrder(int i) {
    i++;
    switch (i % 100) {
    case 11:
    case 12:
    case 13:
      return i + "th";
    default:
      return i + ORDINAL_SUFFIXES[i % 10];
    }
  }


  protected void checkArgsSize(ObjectInspector[] arguments, int min, int max)
      throws UDFArgumentLengthException {
    if (arguments.length < min || arguments.length > max) {
      StringBuilder sb = new StringBuilder();
      sb.append(getFuncName());
      sb.append(" requires ");
      if (min == max) {
        sb.append(min);
      } else {
        sb.append(min).append("..").append(max);
      }
      sb.append(" argument(s), got ");
      sb.append(arguments.length);
      throw new UDFArgumentLengthException(sb.toString());
    }
  }


  protected void checkArgPrimitive(ObjectInspector[] arguments, int i)
      throws UDFArgumentTypeException {
    ObjectInspector.Category oiCat = arguments[i].getCategory();
    if (oiCat != ObjectInspector.Category.PRIMITIVE) {
      throw new UDFArgumentTypeException(i, getFuncName() + " only takes primitive types as "
          + getArgOrder(i) + " argument, got " + oiCat);
    }
  }

  protected void checkArgGroups(ObjectInspector[] arguments, int i, PrimitiveCategory[] inputTypes,
      PrimitiveObjectInspectorUtils.PrimitiveGrouping... grps) throws UDFArgumentTypeException {
    PrimitiveCategory inputType = ((PrimitiveObjectInspector) arguments[i]).getPrimitiveCategory();
    for (PrimitiveObjectInspectorUtils.PrimitiveGrouping grp : grps) {
      if (PrimitiveObjectInspectorUtils.getPrimitiveGrouping(inputType) == grp) {
        inputTypes[i] = inputType;
        return;
      }
    }
    // build error message
    StringBuilder sb = new StringBuilder();
    sb.append(getFuncName());
    sb.append(" only takes ");
    sb.append(grps[0]);
    for (int j = 1; j < grps.length; j++) {
      sb.append(", ");
      sb.append(grps[j]);
    }
    sb.append(" types as ");
    sb.append(getArgOrder(i));
    sb.append(" argument, got ");
    sb.append(inputType);
    throw new UDFArgumentTypeException(i, sb.toString());
  }



  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    checkArgsSize(arguments, 1, 1);

    checkArgPrimitive(arguments, 0);

    checkArgGroups(arguments, 0, inputTypes, STRING_GROUP);

    obtainStringConverter(arguments, 0, inputTypes, converters);

    ObjectInspector outputOI = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    return outputOI;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    String val = getStringValue(arguments, 0, converters);
    if (val == null) {
      return null;
    }

    String valCap = WordUtils.capitalizeFully(val, new char[] { '-', ',', ' ' });
    output.set(valCap);
    return output;
  }

  @Override
  public String getDisplayString(String[] children) {
    return getStandardDisplayString(getFuncName(), children);
  }

//  @Override
  protected String getFuncName() {
    return "initcap";
  }
}