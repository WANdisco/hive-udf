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

package org.apache.hadoop.hive.ql.udf;

import org.apache.commons.lang.WordUtils;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * UDFInitCap.
 *
 */
@Description(name = "initcap", value = "_FUNC_(str) - Returns str, with the first letter of each word in uppercase,"
		+ " all other letters in lowercase. Words are delimited by white space.", extended = "Example:\n"
				+ " > SELECT _FUNC_('tHe soap') FROM src LIMIT 1;\n" + " 'The Soap'")
public class UDFInitCap extends UDF {

	private final Text t = new Text();

	public Text evaluate(Text s) {
		if (s == null) {
			return null;
		}

		t.set(WordUtils.capitalizeFully(s.toString()));
		return t;
	}
}
