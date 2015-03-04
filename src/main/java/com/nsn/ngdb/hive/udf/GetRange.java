package com.nsn.ngdb.hive.udf;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;




public class GetRange extends GenericUDF {
	
	public static final Log LOG = LogFactory.getLog(GetRange.class);
	
	private  Map<String, HashMap<String, ArrayList<Float>>> binTableMap = null;
	
	private static final String DB_URL = "db_url";

	private static final String DB_Driver = "db_driver";

	private static String NO_RANGE_FOUND = "No Range Found";
	
	private static String CEI_BIN_QUERY ="cei_bin" ;
	
	private static String CQI_BIN_QUERY ="cqi_bin";
	
	private static Properties prop = new Properties();
	
	private Text text = new Text();
	
	
	private void loadMap() throws SQLException{
			loadProperties();
			cacheBintable(prop.getProperty(CQI_BIN_QUERY));
			cacheBintable(prop.getProperty(CEI_BIN_QUERY));
	}
	
	private GenericUDFUtils.ReturnObjectInspectorResolver returnOIResolver;


	private ObjectInspector returnInspector;

	
//	private String[] cei = {"AGE","SMS_CEI_INDEX_SAT_LEVEL","CEI_INDEX_SAT_LEVEL","CEI_INDEX","VOICE_CEI_INDEX_SAT_LEVEL","SUBS_COUNT","DATA_CEI_INDEX_SAT_LEVEL"};
//	
//	private String[] cqi = {"B1","B2"};
	
	
	@Override
	public ObjectInspector initialize(ObjectInspector[] arguments)
			throws UDFArgumentException {
			LOG.debug("Enter into initilize ......");

		if (arguments.length != 2) {
			throw new UDFArgumentLengthException(
					"The operator 'Range' accepts 2 arguments.");
		}
		binTableMap = new HashMap<String, HashMap<String, ArrayList<Float>>>();
		returnInspector = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
		returnOIResolver = new GenericUDFUtils.ReturnObjectInspectorResolver(
				true);
		returnOIResolver.update(arguments[0]);
		returnOIResolver.update(arguments[1]);
		try {
			loadMap();
		} catch (SQLException e) {
			LOG.error("Error :"+e.getMessage());
		}
		LOG.debug("Bin Table Map Object:"+binTableMap);
		return returnInspector;
		
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public Object evaluate(DeferredObject[] arguments)	throws HiveException {
		if (arguments != null && arguments.length > 0) {
			
			if (arguments[0].get() != null && arguments[1].get() != null) {
				double fieldValue = Double.parseDouble(arguments[1].get().toString());
				String binType =  arguments[0].get().toString();
			
				 if(binTableMap.get(binType) != null){
				    	HashMap<String, ArrayList<Float>> binValues = binTableMap.get(binType);
				    	Iterator iterator = binValues.entrySet().iterator();
				    	while (iterator.hasNext()) {
				    		Map.Entry pairs = (Map.Entry)iterator.next();
				            ArrayList<Float> valuePair =(ArrayList<Float>) pairs.getValue();
				            if(fieldValue >= valuePair.get(0) && fieldValue <= valuePair.get(1)){
				            	text.set(pairs.getKey().toString());
//				            	text.set(valuePair.get(0) + "-" + valuePair.get(1));
			            		LOG.debug("Range Returning:"+text);
				            	return text;
				            }
				    	}
				    	 
				 }
				
			}
		 }
		 text.set(NO_RANGE_FOUND);
		 return text;
	}

	@Override
	public String getDisplayString(String[] children) {
		StringBuilder sb = new StringBuilder();

		sb.append("returns the number");
		sb.append(children[0]);
		sb.append(" in tens range ");
		return sb.toString();
	}



	private  void cacheBintable(String query) throws SQLException  {
		 Connection conn = null;
		 Statement st = null;
		 ResultSet result = null;
		 try{
			 String url = prop.getProperty(DB_URL);
			 String driver = prop.getProperty(DB_Driver);
			 Class.forName(driver);
			 conn =  DriverManager.getConnection(url, "", "");
			 st = conn.createStatement();
			 result = st.executeQuery(query);
			 while (result.next()) {
				 HashMap<String, ArrayList<Float>> innerlist = new HashMap<String, ArrayList<Float>>();
				 ArrayList<Float> list = new ArrayList<Float>();
				 list.add(result.getFloat(3));
				 list.add(result.getFloat(4));
				 innerlist.put(result.getString(2),list);
				 if (binTableMap.get(result.getString(1)) == null){
					  binTableMap.put(result.getString(1),innerlist);
				 }else{
					 binTableMap.get(result.getString(1)).put(result.getString(2), list);
				 }
		    }
			}catch(Exception e){
					  LOG.error("Error :"+e.getMessage());
			}
		 finally{
		     LOG.debug("Closing Connection...");
			 if (result != null)
			 {
				 result.close();
			 }
			 if (st != null)
			 {
				 st.close() ;
			 }
			 if(conn != null)
			 {
				 conn.close() ;
			 }

		 }
	}
	
	
	private static void loadProperties() {
	    LOG.debug("Loading Properties file.....");
		InputStream input = null;
		try {
			input = new FileInputStream("/usr/local/hiveconf-hdp1/hive-config.properties");
			//input = new FileInputStream("hive-config.properties");
			// load a properties file
			prop.load(input);
		
		} catch (IOException ex) {
			ex.printStackTrace();
		} finally {
			if (input != null) {
				try {
					input.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	 
	  }
	

	
	@SuppressWarnings("resource")
	public static void main(String[] args) throws HiveException, SQLException{
//		loadProperties();
		GetRange getRange = new GetRange();
		getRange.loadMap();
		   Object result = getRange.evaluate(new DeferredObject[]{new DeferredJavaObject("B2"), new DeferredJavaObject(6145)});
		   System.out.println("result:"+result);
//		System.out.println("Age                      :" +getRange(32,"AGE"));
//		System.out.println("SMS_CEI_INDEX_SAT_LEVEL  :" +getRange(32,"SMS_CEI_INDEX_SAT_LEVEL"));
//		System.out.println("CEI_INDEX_SAT_LEVEL      :" +getRange(40,"CEI_INDEX_SAT_LEVEL"));
//		System.out.println("CEI_INDEX                :" +getRange(40,"CEI_INDEX"));
//		System.out.println("VOICE_CEI_INDEX_SAT_LEVEL:" +getRange(40,"VOICE_CEI_INDEX_SAT_LEVEL"));
//		System.out.println("SUBS_COUNT               :" +getRange(40,"SUBS_COUNT"));
//		System.out.println("DATA_CEI_INDEX_SAT_LEVEL :" +getRange(6146,"DATA_CEI_INDEX_SAT_LEVEL"));
//		System.out.println("B1                       :" +getRange(3074,"B1"));
//		System.out.println("B2                       :" +getRange(46,"B2"));
	}

}
