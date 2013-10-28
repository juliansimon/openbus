/*
* Copyright 2013 Produban
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package com.produban.openbus.processor.util;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Formatting useful for date
 *
 */
public class FormatUtil {

	private static Logger LOG = LoggerFactory.getLogger(FormatUtil.class);
			
	private static String DATE_FORMAT_LOG = "[dd/MMM/yyyy:HH:mm:ss+0200]";
	private static String DATE_FORMAT_SECOND = "yyyyMMddHHmmss";
	private static String DATE_FORMAT_MINUTE = "yyyyMMddHHmm";
	private static String DATE_FORMAT_HOUR = "yyyyMMddHH";
	private static String DATE_FORMAT_DAY = "yyyyMMdd";

	public static String getDateFormat(String dateLogs, String oldFormatterStr, String newFormatterStr) {	
        DateFormat oldFormatter = new SimpleDateFormat(oldFormatterStr);
		DateFormat newFormatter = new SimpleDateFormat(newFormatterStr);
        Date oldDate = null;
		
		try {
			oldDate = (Date)oldFormatter.parse(dateLogs);
		} catch (ParseException pe) {
			LOG.error("Error in parse. OldFormatter: " + oldFormatterStr + " newFormatter " + newFormatterStr + " " + pe);			 
		}
	    	    
	    return newFormatter.format(oldDate);
	}
	
	public static String getDateInFormatSecond(String dateLogs) {	    
	    return getDateFormat(dateLogs, DATE_FORMAT_LOG, DATE_FORMAT_SECOND);
	}	
	
	public static String getDateInFormatMinute(String dateLogs) {	    
	    return getDateFormat(dateLogs, DATE_FORMAT_LOG, DATE_FORMAT_MINUTE);
	}
	
	public static String getDateInFormatHour(String dateLogs) {	    
	    return getDateFormat(dateLogs, DATE_FORMAT_LOG, DATE_FORMAT_HOUR);
	}
	
	public static String getDateInFormatDay(String dateLogs) {	    
	    return getDateFormat(dateLogs, DATE_FORMAT_LOG, DATE_FORMAT_DAY);
	}	
		
	/***
	 * 
	 * @param request
	 * @return
	 */
	public static String getRequestFormat(String request) {		
		request = request.replaceAll("GET_/", "");		
		return request.replaceAll("\"", "");
	}
}