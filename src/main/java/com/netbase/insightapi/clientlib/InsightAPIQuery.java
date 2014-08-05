
package com.netbase.insightapi.clientlib;

import java.io.IOException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

import javax.servlet.http.HttpServletResponse;

import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.HttpVersion;
import org.apache.http.client.HttpRequestRetryHandler;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.conn.params.ConnRoutePNames;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.EntityUtils;
import org.json.simple.JSONValue;

/*
 Copyright 2014, NetBase Solutions, Inc.

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

/**
 * The InsightAPIQuery class represents the operation name (e.g.
 * "insightCount"), parameters and result state for a single query. It contains
 * support methods for creating and manipulating the URL-encoded arguments, and
 * leverages Apache's HTTPComponents library to make the requests.
 * 
 * A typical use is to create an InsightAPIQuery with one of the operation
 * names, set some parameters, and run the query from either the run() or
 * start() method of UserChannel.
 */
public class InsightAPIQuery
{
    protected static final String PUBLISHED_DATE = "publishedDate";
    protected static final String PUBLISHED_TIMESTAMP = "timestamp";
    protected static final String ALERT_DATE = "alertDatetime";
    protected static final String ALERT_TIMESTAMP = "alertTimestamp";

    protected static final String REALTIME = "realTime";
    protected static final String URL_ARG_ENCODING_CHARSET = "UTF-8";

    /*
     * The following InsightAPI operations do not affect the rate limit, and may
     * be run at any time. All others will be assumed to be controlled by either
     * the general or realtime rate limit, depending on the "realTime"
     * parameter.
     */
    protected static final List<String> rateLimitFreebies;
    static {
        rateLimitFreebies = new ArrayList<String>();
        rateLimitFreebies.add("helloWorld");
        rateLimitFreebies.add("topics");
        rateLimitFreebies.add("themes");
    }

    // timespan of data accessible with the realTime=true parameter
    protected static final long ALERT_DATE_REALTIME_LIMIT = 24L * 60L * 60L * 1000L;

    // 2012-01-01 00:00:00.000 GMT
    protected static final long NETBASE_EPOCH_TIMESTAMP = 1325376000000L;

    // this is not static because DateFormat objects are not thread safe;
    // we create one per query just to avoid having to synchronize on a
    // single copy
    protected SimpleDateFormat dateFormat;

    // serial numbers for the InsightAPI queries are for convenience in
    // logging. The API will run several of them at a time, if the user
    // wishes and the rate limit allows. Without serial numbers in the
    // log messages, connecting requests and results (and errors) can
    // be difficult.
    protected static AtomicLong masterSerial = new AtomicLong(1000L);
    protected long serial = masterSerial.incrementAndGet();

    // URL and its parameters
    protected String urlBase;
    protected Map<String, List<String>> parameters = new TreeMap<String, List<String>>();
    protected String op;
    protected boolean mustRespectRateLimit = true;

    // settings for proxy connectivity (default: none)
    protected String proxyServer;
    protected int proxyPort = 3128;
    protected String proxyScheme = "http";

    // timeout and retry settings
    protected int timeout = 30000;
    protected int maxRetries = 3;

    // query result
    protected String responseContent;
    protected Object parsedContent;
    protected Map<String, String> headers = new TreeMap<String, String>();
    protected int statusCode = -1;
    protected String statusString;
    protected long elapsedMs;

    protected boolean debug;

    /**
     * Create a new InsightAPI Query with the specified operation.
     * 
     * @param op
     */
    public InsightAPIQuery (String op)
    {
        this();
        setOp(op);
    }

    /**
     * Create a new InsightAPI Query with the default ("helloWorld") operation.
     */
    public InsightAPIQuery ()
    {
        setOp("helloWorld");
        // NB: The InsightAPI accepts several different datetime formats,
        // but this is one of them. We use it here only for composing
        // time arguments given java Dates and long times.
        dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
    }

    /**
     * Clone an InsightAPI query, including all its control parameters, but not
     * its result.
     * 
     * @param other
     */
    public InsightAPIQuery (InsightAPIQuery other)
    {
        this(other.op);
        for (Map.Entry<String, List<String>> ent : other.parameters.entrySet()) {
            for (String value : ent.getValue())
                addParameter(ent.getKey(), value);
        }
        urlBase = other.urlBase;
        proxyServer = other.proxyServer;
        proxyPort = other.proxyPort;
        proxyScheme = other.proxyScheme;
        timeout = other.timeout;
        maxRetries = other.maxRetries;
        debug = other.debug;
    }

    /**
     * Set the operation (eg "helloWorld" or "retrieveDocuments") that this
     * InsightAPI query will perform
     * 
     * @param op
     */
    public void setOp (String op)
    {
        this.op = op;
        mustRespectRateLimit = !rateLimitFreebies.contains(op);
    }

    // =========================================================================
    // Date range setting methods. These facilitate setting these ranges, by
    // providing the formatting and other assumptions that the API requires, and
    // sets the "realTime" property in conjunction with them.
    //
    // If the user provides either java.util.Dates or long time values, we
    // format these as string arguments, because the full millisecond range is
    // supported in the NetBase index. If the user provides Integer timestamp
    // values, we give these to the server as-is. These have 1/10 second
    // resolution, but probably came from the server, and it makes sense to
    // return them in the same scale.
    // =========================================================================

    /**
     * Sets the query to be restricted to a specific published date range. The
     * operation removes any existing published date range. If d1 is null,
     * nothing else is done. If d2 is null, the endpoint of the range is not
     * set.
     * 
     * @param d1
     * @param d2
     */
    public void setPublishedDateRange (java.util.Date d1, java.util.Date d2)
    {
        removeParameters(PUBLISHED_DATE);
        if (d1 == null)
            return;
        setParameter(PUBLISHED_DATE, d1);
        if (d2 != null)
            addParameter(PUBLISHED_DATE, d2);
        setRealtime(d1);
    }

    /**
     * Sets the query to be restricted to a specific published date range. The
     * operation removes any existing published date range. If d1 is
     * Long.MIN_VALUE, nothing else is done. If d2 is Long.MAX_VALUE, the
     * endpoint of the range is not set.
     * 
     * @param d1
     * @param d2
     */
    public void setPublishedDateRange (long d1, long d2)
    {
        setPublishedDateRange(d1 == Long.MIN_VALUE ? null
                : new java.util.Date(d1), d2 == Long.MAX_VALUE ? null
                : new java.util.Date(d2));
    }

    /**
     * Sets the query to be restricted to a specific published date range. The
     * operation removes any existing published date range. If d1 is
     * Integer.MIN_VALUE, nothing else is done. If d2 is Integer.MAX_VALUE, the
     * endpoint of the range is not set.
     * 
     * @param d1
     * @param d2
     */
    public void setPublishedTimestampRange (int d1, int d2)
    {
        removeParameters(PUBLISHED_TIMESTAMP);
        if (d1 == Integer.MIN_VALUE)
            return;
        setParameter(PUBLISHED_TIMESTAMP, d1);
        if (d2 != Integer.MAX_VALUE)
            addParameter(PUBLISHED_TIMESTAMP, d2);
        setRealtime(new java.util.Date(timestampToLongTime(d1)));
    }

    /**
     * Sets the query to be restricted to a specific alert date range. The
     * operation removes any existing alert date range. If d1 is null, nothing
     * else is done. If d2 is null, the endpoint of the range is not set.
     * 
     * @param d1
     * @param d2
     */
    public void setAlertDateRange (java.util.Date d1, java.util.Date d2)
    {
        removeParameters(ALERT_DATE);
        if (d1 == null)
            return;
        setParameter(ALERT_DATE, d1);
        if (d2 != null)
            addParameter(ALERT_DATE, d2);
        setRealtime(d1);
    }

    /**
     * Sets the query to be restricted to a specific alert date range. The
     * operation removes any existing alert date range. If d1 is Long.MIN_VALUE,
     * nothing else is done. If d2 is Long.MAX_VALUE, the endpoint of the range
     * is not set.
     * 
     * @param d1
     * @param d2
     */
    public void setAlertDateRange (long d1, long d2)
    {
        setAlertDateRange(d1 == Long.MIN_VALUE ? null : new java.util.Date(d1),
                          d2 == Long.MAX_VALUE ? null : new java.util.Date(d2));
    }

    /**
     * Sets the query to be restricted to a specific alert date range. The
     * operation removes any existing alert date range. If d1 is
     * Integer.MIN_VALUE, nothing else is done. If d2 is Integer.MAX_VALUE, the
     * endpoint of the range is not set.
     * 
     * @param d1
     * @param d2
     */
    public void setAlertTimestampRange (int d1, int d2)
    {
        removeParameters(ALERT_TIMESTAMP);
        if (d1 == Integer.MIN_VALUE)
            return;
        setParameter(ALERT_TIMESTAMP, d1);
        if (d2 != Integer.MAX_VALUE)
            addParameter(ALERT_TIMESTAMP, d2);
        setRealtime(new java.util.Date(timestampToLongTime(d1)));
    }

    /**
     * Sets the query's "realTime" parameter depending on the value of the
     * argument, which is assumed to be the earliest time the query will
     * reference.
     * 
     * @param d
     */
    public void setRealtime (java.util.Date d)
    {
        boolean realtime = System.currentTimeMillis() - d.getTime() < ALERT_DATE_REALTIME_LIMIT;
        setRealtime(realtime);
    }

    /**
     * Sets the query's "realTime" parameter depending on the value of the
     * argument.
     * 
     * @param d
     */
    public void setRealtime (boolean realtime)
    {
        setParameter(REALTIME, realtime);
    }

    /**
     * Returns true if the query has a "realTime" parameter, and its value is
     * "true".
     * 
     * @return
     */
    public boolean isRealtime ()
    {
        List<String> realtime = getParameters(REALTIME);
        if (realtime == null || realtime.size() == 0)
            return (false);
        return (realtime.get(0).equalsIgnoreCase("true"));
    }

    /**
     * Utility routine that converts a NetBase timestamp value into a java time
     * (milliseconds since the java epoch). Preserves MIN_VALUE and MAX_VALUE
     * conventions.
     * 
     * @param timestamp
     * @return
     */
    public static long timestampToLongTime (int timestamp)
    {
        switch (timestamp) {
            case Integer.MIN_VALUE:
                return (Long.MIN_VALUE);
            case Integer.MAX_VALUE:
                return (Long.MAX_VALUE);
            default:
                return (((long)timestamp) * 100 + NETBASE_EPOCH_TIMESTAMP);
        }
    }

    /**
     * Utility routine that converts a java time to a NetBase timestamp.
     * Preserves MIN_VALUE and MAX_VALUE conventions.
     * 
     * @param time
     * @return
     */
    public static int longTimeToTimestamp (long time)
    {
        if (time == Long.MIN_VALUE)
            return (Integer.MIN_VALUE);
        if (time == Long.MAX_VALUE)
            return (Integer.MAX_VALUE);
        return ((int)((time - NETBASE_EPOCH_TIMESTAMP) / 100L));
    }

    /**
     * Returns true if the query is complete. An InsightAPIQuery is not complete
     * unless it's finished, and it did not return a rate limit exceeded
     * complaint. This is distinct from "succeeded", which requires that
     * statusCode == SC_OK (200).
     */
    public boolean isComplete ()
    {
        return (statusCode != -1 && (statusCode != HttpServletResponse.SC_FORBIDDEN || !responseContent.toLowerCase()
                                                                                                       .contains("rate limit")));
    }

    /**
     * Blocks the current thread until this Query is complete.
     * 
     * @throws InterruptedException
     */
    public void waitForCompletion ()
        throws InterruptedException
    {
        while (!isComplete()) {
            synchronized (this) {
                this.wait();
            }
        }
    }

    /**
     * May be overridden to use a different JSON Parser than the one included in
     * this library.
     */
    public void parseResponseContent ()
    {
        parsedContent = JSONValue.parse(responseContent);
    }

    // ============================
    // Methods to manage parameters
    // ============================

    /**
     * Add a parameter, by name, to the query. If the parameter already exists,
     * an additional sequential value will be appended to the query. The value
     * of the parameter is converted to string form in this method.
     * 
     * @param name
     * @param value
     */
    public void addParameter (String name, Object value)
    {
        // null values are ignored.
        if (value != null) {
            List<String> values = parameters.get(name);
            if (values == null) {
                values = new ArrayList<String>();
                parameters.put(name, values);
            }

            values.add(parameterToString(value));
        }
    }

    /**
     * Convert a parameter from an Object to a string. This method may be
     * overridden to add more specialized encoding of parameters.
     * 
     * The default implementation is not trivial; it provides proper conversion
     * for java.util.Date objects. User code that overrides this method should
     * continue to call it for date classes.
     * 
     * @param param
     * @return
     */
    public String parameterToString (Object param)
    {
        if (param instanceof java.util.Date)
            return (dateFormat.format(param));
        else
            return (param.toString());
    }

    /**
     * Remove all values of this parameter, and then add this one.
     * 
     * @param name
     * @param value
     */
    public void setParameter (String name, Object value)
    {
        removeParameters(name);
        addParameter(name, value);
    }

    public void addParameter (String name, long value)
    {
        addParameter(name, Long.valueOf(value));
    }

    public void setParameter (String name, long value)
    {
        removeParameters(name);
        addParameter(name, value);
    }

    public void addParameter (String name, int value)
    {
        addParameter(name, Integer.valueOf(value));
    }

    public void setParameter (String name, String value)
    {
        removeParameters(name);
        addParameter(name, value);
    }

    public void addParameter (String name, boolean value)
    {
        addParameter(name, Boolean.valueOf(value));
    }

    public void setParameter (String name, boolean value)
    {
        removeParameters(name);
        addParameter(name, value);
    }

    public void addParameters (String name, Object... values)
    {
        for (Object value : values)
            addParameter(name, value);
    }

    public void addParameters (String name, List<Object> values)
    {
        for (Object value : values)
            addParameter(name, value);
    }

    /**
     * Removes all current values for this named parameter.
     * 
     * @param name
     */
    public void removeParameters (String name)
    {
        parameters.remove(name);
    }

    /**
     * Returns the current values for this named parameter.
     * 
     * @param name
     * @return
     */
    public List<String> getParameters (String name)
    {
        return (parameters.get(name));
    }

    /**
     * Returns the current URL that will be sent to the server when the query is
     * run. Proper URL escape codes are added to parameter values.
     * 
     * @return
     */
    public String getUrl ()
    {
        StringBuffer result = new StringBuffer();
        result.append(urlBase);
        String delim = "?";
        for (Map.Entry<String, List<String>> ent : parameters.entrySet()) {
            for (String value : ent.getValue()) {
                result.append(delim);
                delim = "&";
                result.append(ent.getKey());
                result.append("=");
                String encodedArg = value;
                try {
                    encodedArg = URLEncoder.encode(value,
                                                   URL_ARG_ENCODING_CHARSET);
                }
                catch (Exception ue) {
                }
                result.append(encodedArg);
            }
        }
        return (result.toString());
    }

    /**
     * Make a diligent attempt to run the query on the server, retrying in the
     * case of timeouts. Passes through any non-retriable exceptions, but does
     * not check that the operation was "successful".
     * 
     * This entry point should not be used directly by application programs.
     * Queries should be run with either UserChannel.run() or
     * UserChannel.start().
     * 
     * @param userChannel
     * @throws Exception
     */
    protected void runQuery (UserChannel userChannel)
        throws Exception
    {
        DefaultHttpClient httpclient = null;
        try {

            statusCode = -1;
            statusString = null;

            String url = getUrl();

            if (debug)
                userChannel.logInfo(serial + " requesting: "
                        + URLDecoder.decode(url, URL_ARG_ENCODING_CHARSET));

            httpclient = new DefaultHttpClient();

            HttpGet httpget = new HttpGet(url);

            httpclient.getParams().setParameter("http.protocol.version",
                                                HttpVersion.HTTP_1_1);
            httpclient.getParams().setParameter("http.socket.timeout",
                                                new Integer(timeout));
            httpclient.getParams()
                      .setParameter("http.protocol.content-charset",
                                    URL_ARG_ENCODING_CHARSET);
            httpclient.getParams().setParameter("http.tcp.nodelay",
                                                Boolean.TRUE);
            httpclient.getParams().setParameter("http.connection.timeout",
                                                new Integer(timeout));

            // set the proxy properties, if any
            if (proxyServer != null) {
                httpclient.getParams()
                          .setParameter(ConnRoutePNames.DEFAULT_PROXY,
                                        new HttpHost(proxyServer,
                                                     proxyPort,
                                                     proxyScheme));
            }
            // set retry handler
            httpclient.setHttpRequestRetryHandler(new RetryHandler(url,
                                                                   userChannel));

            // run the request
            long startTime = System.currentTimeMillis();
            HttpResponse response = httpclient.execute(httpget);

            // get result statistics
            elapsedMs = (System.currentTimeMillis() - startTime);
            statusCode = response.getStatusLine().getStatusCode();
            statusString = response.getStatusLine().getReasonPhrase();

            // gather the internet headers
            for (Header header : response.getAllHeaders()) {
                // force the headers to lower case. As they come from the
                // server, they are CamelCase, e.g.
                // X-RealTimeConcurrentRateLimit-Max
                headers.put(header.getName().toLowerCase(), header.getValue());
            }

            // retrieve the content
            String charset = "UTF-8";
            if (response.getEntity() != null
                    && response.getEntity().getContentEncoding() != null) {
                charset = response.getEntity().getContentEncoding().getValue();
            }
            if (response.getEntity() != null) {
                responseContent = EntityUtils.toString(response.getEntity(),
                                                       charset);

                if (debug)
                    userChannel.logInfo(serial + " result: "
                            + responseContent.length()
                            + " characters, status code: " + statusCode
                            + ", statusString: " + statusString);
            }
        }

        finally {
            if (httpclient != null) {
                // close connection
                httpclient.getConnectionManager().shutdown();
            }
        }

    }

    protected class RetryHandler implements HttpRequestRetryHandler
    {
        String requestInfo;
        UserChannel userChannel;

        public RetryHandler (String requestInfo, UserChannel userChannel)
        {
            this.requestInfo = requestInfo;
            this.userChannel = userChannel;
        }

        public boolean retryRequest (IOException exception,
                                     int executionCount,
                                     HttpContext context)
        {
            userChannel.logWarning(serial + " request " + executionCount
                    + " times, exception " + exception.toString() + "  "
                    + requestInfo);
            if (executionCount > maxRetries) {
                userChannel.logWarning(serial + " retry count exceeds limit: "
                        + maxRetries);
                return false;
            }
            return true;
        }
    }

    /**
     * Throws an InsightAPIQueryException if the query was not successful.
     * 
     * @throws InsightAPIQueryException
     */
    public void checkSuccess ()
        throws InsightAPIQueryException
    {
        if (!isSuccessful())
            throw new InsightAPIQueryException(statusCode,
                                               statusString,
                                               responseContent);
    }

    public static class InsightAPIQueryException extends Exception
    {
        private static final long serialVersionUID = -6258401126258790233L;
        protected int statusCode;
        protected String statusString;
        protected String responseContent;

        public InsightAPIQueryException (int statusCode,
                                         String statusString,
                                         String responseContent)
        {
            this.statusCode = statusCode;
            this.statusString = statusString;
            this.responseContent = responseContent;
        }

        public String toString ()
        {
            return (getClass().getName() + " " + statusCode + " "
                    + statusString + " " + responseContent);
        }
    }

    // ============
    // Bean Methods
    // ============

    protected void setUrlBase (String urlBase)
    {
        this.urlBase = urlBase;
    }

    protected String getUrlBase ()
    {
        return (urlBase);
    }

    /**
     * Sets the server name, port number and scheme name for accessing the
     * NetBase InsightAPI through a network proxy.
     * 
     * @param proxyServer
     *            If not null, the server name
     * @param proxyPort
     *            If not -1, the port number; defaults to 3128
     * @param proxyScheme
     *            If not null, the proxy scheme; defaults th "http"
     */
    public void setProxy (String proxyServer, int proxyPort, String proxyScheme)
    {
        if (proxyServer != null)
            this.proxyServer = proxyServer;
        if (proxyPort > 0)
            this.proxyPort = proxyPort;
        if (proxyScheme != null)
            this.proxyScheme = proxyScheme;
    }

    /**
     * Sets the number of retries that the query will make before it gives up
     * and sets a failure status code.
     * 
     * @param timeout
     * @param maxRetries
     */
    public void setRetries (int timeout, int maxRetries)
    {
        this.timeout = timeout;
        this.maxRetries = maxRetries;
    }

    /**
     * returns whether the query is logging debug messages
     * 
     * @return
     */
    public boolean isDebug ()
    {
        return debug;
    }

    /**
     * Sets whether the query is logging debug messages
     * 
     * @param debug
     */
    public void setDebug (boolean debug)
    {
        this.debug = debug;
    }

    /**
     * After the query is run, returns the value of the named internet header as
     * delivered by the server.
     * 
     * @param name
     * @return
     */
    public String getHeaderValue (String name)
    {
        return (headers.get(name));
    }

    /**
     * After the query is run, returns a map of all internet headers as
     * delivered by the server. This map will be updated asynchronously if the
     * query is still being run.
     * 
     * @return
     */
    public Map<String, String> getHeaders ()
    {
        return (headers);
    }

    /**
     * Returns true if the query was successful, ie returned a status code of
     * 200.
     * 
     * @return
     */
    public boolean isSuccessful ()
    {
        return (statusCode == HttpServletResponse.SC_OK);
    }

    /**
     * Returns the value (in milliseconds) of the connection timeout parameter
     * used for the query.
     * 
     * @return
     */
    public int getTimeout ()
    {
        return timeout;
    }

    /**
     * Sets the number of milliseconds that the query will be allowed to run
     * before returning a timeout error. Default: 30,000 (30 seconds).
     * 
     * @param timeout
     */
    public void setTimeout (int timeout)
    {
        this.timeout = timeout;
    }

    /**
     * Returns the current value of the proxy server name.
     * 
     * @return
     */
    public String getProxyServer ()
    {
        return proxyServer;
    }

    /**
     * Returns the current value of the proxy port number.
     * 
     * @return
     */
    public int getProxyPort ()
    {
        return proxyPort;
    }

    /**
     * Returns the current value of the proxy scheme name.
     * 
     * @return
     */
    public String getProxyScheme ()
    {
        return proxyScheme;
    }

    /**
     * After the query has been run, returns the number of milliseconds it of
     * elapsed time it took.
     * 
     * @return
     */
    public long getElapsedMs ()
    {
        return elapsedMs;
    }

    /**
     * Returns the current maximum number of times the query will be retried
     * before it fails.
     * 
     * @return
     */
    public int getMaxRetries ()
    {
        return maxRetries;
    }

    /**
     * Returns the entire content returned by the server as its response value.
     * 
     * @return
     */
    public String getResponseContent ()
    {
        return responseContent;
    }

    /**
     * Returns the HTTP Status code (OK=200) provided by the query.
     * 
     * @return
     */
    public int getStatusCode ()
    {
        return statusCode;
    }

    /**
     * Returns the HTTP Status string (OK="OK") provided by the query.
     * 
     * @return
     */
    public String getStatusString ()
    {
        return statusString;
    }

    /**
     * Returns the parsed value (ie some sort of JSON object) provided by the
     * query.
     * 
     * @return
     */
    public Object getParsedContent ()
    {
        return parsedContent;
    }

    /**
     * Returns the operation name (eg "helloWorld") that the query performs.
     * 
     * @return
     */
    public String getOp ()
    {
        return op;
    }

    /**
     * Returns true if this query (as specified by the operation name) must
     * respect the rate limit, or is a rate limit "freebie".
     * 
     * @return
     */
    public boolean isMustRespectRateLimit ()
    {
        return mustRespectRateLimit;
    }

    /**
     * Returns the query's serial number, which will be used in logging.
     * 
     * @return
     */
    public long getSerial ()
    {
        return serial;
    }

}
