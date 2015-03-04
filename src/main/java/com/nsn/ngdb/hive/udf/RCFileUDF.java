package com.nsn.ngdb.hive.udf;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.MapredContext;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.io.CombineHiveInputFormat.CombineHiveInputSplit;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFUtils;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;

public class RCFileUDF extends GenericUDF {
	
private MapredContext context=null;
private static CharsetDecoder decoder;
	
	private GenericUDFUtils.ReturnObjectInspectorResolver returnOIResolver;

	private ObjectInspector returnInspector;
	
	private Text text = new Text();
	
	private int count=0;
	private static final int STRING_BUFFER_SIZE = 16 * 1024;

	
	static{
	    decoder = Charset.forName("UTF-8").newDecoder().
	  	      onMalformedInput(CodingErrorAction.REPLACE).
	  	      onUnmappableCharacter(CodingErrorAction.REPLACE);
	}
	
	@Override
	public void configure(MapredContext context) {
		System.out.println("Inside configure()");
		this.context=context;
		
	}

	@Override
	public Object evaluate(DeferredObject[] arg0) throws HiveException {
		if (count == 0) {
			if (context != null) {

				System.out.println("Is it map?:" + context.isMap());

				JobConf conf = context.getJobConf();
				if (conf != null) {
					System.out.println("map input file name:"
							+ conf.get("map.input.file"));
					System.out.println("map split file name:"
							+ conf.get("mapred.job.split.file"));
				} else {
					System.out.println("Job Conf is null");
				}

				if (context.getReporter() != null) {
					InputSplit inputSplit = context.getReporter()
							.getInputSplit();
					if (inputSplit != null) {
						
						try {
							CombineHiveInputSplit chis=(CombineHiveInputSplit)inputSplit;
							int i=0;
							LongWritable key=new LongWritable(1);
							BytesRefArrayWritable value=new BytesRefArrayWritable();
							for(Path path: chis.getPaths()) {

								int cnt=1;
								CustomRCFileRecordReader<LongWritable, BytesRefArrayWritable> rcFileReader=new CustomRCFileRecordReader<LongWritable, BytesRefArrayWritable>(conf,chis.getOffset(i),chis.getLength(i),path);
								System.out.println("Reading File["+i+"], Path:"+path.toString());
								while(rcFileReader.next(key, value)) {
														
									System.out.println(cnt+":"+key+":"+printRecord(value).toString());
									cnt++;
								}
								i++;
							}
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}
			}
		}
		
		if (arg0[0].get() != null && arg0[1].get()!=null) {
			text.set(arg0[0].get().toString()+","+arg0[1].get().toString());
			System.out.println("*val*:"+text.toString()+",count:"+count);
		}
		else if (arg0[0].get() != null) {
			text.set(arg0[0].get().toString());
			System.out.println("*val*:"+text.toString()+",count:"+count);
		}		
		else if (arg0[1].get() != null) {
			text.set(arg0[1].get().toString());
			System.out.println("*val*:"+text.toString()+",count:"+count);
		}			
		count++;
		return text;
	}

	private StringBuilder printRecord(BytesRefArrayWritable value)
	throws IOException {
		StringBuilder buf = new StringBuilder(STRING_BUFFER_SIZE);		
		int n = value.size();
		if (n > 0) {
			BytesRefWritable v = value.unCheckedGet(0);
			ByteBuffer bb = ByteBuffer.wrap(v.getData(), v.getStart(),
					v.getLength());

			buf.append(decoder.decode(bb));

			for (int i = 1; i < n; i++) {

				// do not put the TAB for the last column
				buf.append(",");
				v = value.unCheckedGet(i);
				bb = ByteBuffer.wrap(v.getData(), v.getStart(), v.getLength());
				buf.append(decoder.decode(bb));
			}
			//buf.append(NEWLINE);

		}
		return buf;
	}
	
	@Override
	public String getDisplayString(String[] arg0) {
		StringBuilder sb = new StringBuilder();
		sb.append(arg0[0]);
		return sb.toString();
	}

	@Override
	public ObjectInspector initialize(ObjectInspector[] arg0)
			throws UDFArgumentException {
		returnInspector = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
		returnOIResolver = new GenericUDFUtils.ReturnObjectInspectorResolver(
				true);
		returnOIResolver.update(arg0[0]);
		returnOIResolver.update(arg0[1]);
		
		return returnInspector;
	}

}

