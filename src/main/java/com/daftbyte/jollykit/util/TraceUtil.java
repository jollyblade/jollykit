package com.daftbyte.jollykit.util;

import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * @author Marton Szabo
 */
public class TraceUtil {


    /**
     * Prints the stackTrace to a {@link java.io.StringWriter} and returns the printed string
     *
     * @param t the {@link Throwable} to be printed to a {@link String}
     * @return not null <code>String</code>, value is 'null' if t is null
     */
    public static String stackTraceToString(final Throwable t) {

        if (t == null)
            return String.valueOf(t);

        final StringWriter sw = new StringWriter();
        final PrintWriter pw = new PrintWriter(sw, true);
        t.printStackTrace(pw);
        pw.flush();
        return sw.toString();
    }
}
