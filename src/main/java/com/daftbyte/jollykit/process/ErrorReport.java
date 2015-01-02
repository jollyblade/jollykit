package com.daftbyte.jollykit.process;

import java.util.HashMap;
import java.util.Map;
import com.daftbyte.jollykit.util.TraceUtil;

/**
 * Error report to gather exceptions, and log them out once, if the same error message is used again and again,
 * it is aggregated. Would be nice to recognize not exact matches only, but similar messages too
 *
 * @author Marton Szabo jollyblade@gmail.com
 */
public class ErrorReport {

	private int counter = 0;

	private final String name;

	private final Map<String, Exception> errors = new HashMap<>();

	private final Map<String, Integer> errorCounts = new HashMap<>();

	public ErrorReport(final String name) {
		this.name = name;
	}

	public void append(final String message) {
		Integer errorCount = errorCounts.get(message);
		if (errorCount == null) {
			errorCount = new Integer(1);
			errorCounts.put(message, errorCount);
		}
		else {
			errorCount = new Integer(errorCount.intValue() + 1);
			errorCounts.put(message, errorCount);
		}
		counter++;
	}

	public void append(final String message, final Exception e) {
		Integer errorCount = errorCounts.get(message);
		if (errorCount == null) {
			errorCount = new Integer(1);
			errorCounts.put(message, errorCount);
			errors.put(message, e);
		}
		else {
			errorCount = new Integer(errorCount.intValue() + 1);
			errorCounts.put(message, errorCount);
		}
		counter++;
	}

	@Override
	public String toString() {
		StringBuilder rc = new StringBuilder();
		rc.append(name + " error report, there were " + counter + " errors in the process with the following messages:\n");
		for (final String key : errorCounts.keySet()) {
			final Integer count = errorCounts.get(key);
			final Exception e = errors.get(key);
			if (e != null) {
				rc.append("Error occured " + count + " times: " + key + ", original error: " + e.getMessage()).append("\n");
				rc.append(TraceUtil.stackTraceToString(e));
				rc.append("\n");
			}
			else {
				rc.append("Error occured " + count + " times: " + key).append("\n");
			}
		}
		return rc.toString();
	}

	public boolean hasErrors() {
		return counter > 0;
	}
}
