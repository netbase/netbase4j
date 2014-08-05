
package com.netbase.insightapi.clientlib;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import com.netbase.insightapi.clientlib.InsightAPIQuery.InsightAPIQueryException;

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
 * An InsightAPI user channel is a data structure that gathers information about
 * a stream of API calls to a particular server (usually, the default) on behalf
 * of a single user/password combination.
 * 
 * By routing all the API calls through this object, we can track the rate limit
 * output in the NetBase server's response headers, and automatically delay
 * sequential and concurrent requests to avoid breaking the rate limit.
 */
public class UserChannel
{
    protected static final int NUM_FREEBIE_THREADS = 5;

    /** Maximum number of general requests that may be run at the same time */
    public static final int MAX_REALTIME_THREADS = 20;

    /** Maximum number of realtime requests that may be run at the same time */
    public static final int MAX_GENERAL_THREADS = 20;
    protected static final InsightAPIQuery EOQ_QUERY = new InsightAPIQuery("EOQ");

    protected String user;
    protected String password;
    protected String serverBaseURL = System.getProperty("Insight_API_URL",
                                                        "https://api.netbase.com:443/cb/insight-api/2/");

    // queues of queries to be run in each category
    protected LinkedBlockingQueue<InsightAPIQuery> realtimeQueue = new LinkedBlockingQueue<InsightAPIQuery>();
    protected LinkedBlockingQueue<InsightAPIQuery> generalQueue = new LinkedBlockingQueue<InsightAPIQuery>();
    protected LinkedBlockingQueue<InsightAPIQuery> freebieQueue = new LinkedBlockingQueue<InsightAPIQuery>();

    // number of runner threads for each category
    protected AtomicInteger realtimeRunnerThreads = new AtomicInteger(0);
    protected AtomicInteger generalRunnerThreads = new AtomicInteger(0);
    protected AtomicInteger freebieRunnerThreads = new AtomicInteger();

    // last-received values for the rate limit headers
    protected RateLimitStatus rateLimitStatus;

    protected boolean debug = false;

    // ============
    // constructors
    // ============

    /**
     * Construct a user channel, with the username and password provided, and
     * the URL at which the server is to be found. If serverBaseURL is null, the
     * default value will be used. This is available for testing staging
     * servers. The username/password is not tested until an InsightAPIQuery is
     * run on the channel.
     * 
     * @param user
     * @param password
     * @param serverBaseURL
     */
    public UserChannel (String user, String password, String serverBaseURL)
    {
        this(user, password);
        if (serverBaseURL != null)
            this.serverBaseURL = serverBaseURL;
    }

    /**
     * Construct a user channel, with the username and password provided. The
     * username/password is not tested until an InsightAPIQuery is run on the
     * channel.
     * 
     * @param user
     * @param password
     */
    public UserChannel (String user, String password)
    {
        this.user = user;
        this.password = password;

        rateLimitStatus = new RateLimitStatus();
        rateLimitStatus.realtime = new RateLimitStatusItem();
        rateLimitStatus.general = new RateLimitStatusItem();

        // start 1 runner thread for each of realtime and general;
        // once we have headers, we'll start more if needed.
        updateRateLimitStatus(new HashMap<String, String>());

        // start a separate group of runner threads for "freebie"
        // queries, that don't affect the rate limit.
        checkRunnerThreads(freebieRunnerThreads,
                           NUM_FREEBIE_THREADS,
                           freebieQueue,
                           null,
                           "freebie");
    }

    // ========================================================================
    // data structure containing the rate limit information from server headers
    // ========================================================================

    public class RateLimitStatusFrame implements Cloneable
    {
        // initializing max to 1 causes us to start with one runner thread
        // for realtime and general, until we get the first copy of the
        // rate limit headers.
        protected int max = 1;
        protected int remaining = 1;
        protected int reset = 10;
        protected long waitUntilTime = -1;

        protected RateLimitStatusFrame ()
        {
        }

        protected RateLimitStatusFrame (RateLimitStatusFrame other)
        {
            max = other.max;
            remaining = other.remaining;
            reset = other.reset;
            waitUntilTime = other.waitUntilTime;
        }

        public String toString ()
        {
            return ("{" + max + ", " + remaining + ", " + reset + ", "
                    + waitUntilTime + "}");
        }

        /**
         * The largest number of queries that may be made during a time period.
         * 
         * @return
         */
        public int getMax ()
        {
            return max;
        }

        /**
         * The remaining number of queries that may be made before the limit is
         * reached.
         * 
         * @return
         */
        public int getRemaining ()
        {
            return remaining;
        }

        /**
         * If remaining == 0, the number of seconds to wait before making
         * another query
         * 
         * @return
         */
        public int getReset ()
        {
            return reset;
        }

        /**
         * If this value is not -1, it represents the time-of-day when queries
         * may resume.
         * 
         * @return
         */
        public long getWaitUntilTime ()
        {
            return waitUntilTime;
        }

    }

    public class RateLimitStatusItem
    {
        protected RateLimitStatusFrame concurrent = new RateLimitStatusFrame();
        protected RateLimitStatusFrame sequential = new RateLimitStatusFrame();
        protected boolean updated = false;

        protected RateLimitStatusItem ()
        {
        }

        protected RateLimitStatusItem (RateLimitStatusItem other)
        {
            concurrent = new RateLimitStatusFrame(other.concurrent);
            sequential = new RateLimitStatusFrame(other.sequential);
            updated = other.updated;
        }

        public String toString ()
        {
            return ("{sequential=" + sequential + ", concurrent=" + concurrent
                    + ", updated=" + updated + "}");
        }

        /**
         * Status of the concurrent (i.e. short term) rate limit. This is
         * utilized to limit the number of queries that may be run
         * simultaneously.
         * 
         * @return
         */
        public RateLimitStatusFrame getConcurrent ()
        {
            return concurrent;
        }

        /**
         * Status of the sequential (i.e. one-hour) rate limit.
         * 
         * @return
         */
        public RateLimitStatusFrame getSequential ()
        {
            return sequential;
        }

        /**
         * If true, this indicates that we have received at least one update of
         * this data from the server. If false, the values here represent
         * defaults coded into this library (i.e. not from the NetBase server).
         * 
         * @return
         */
        public boolean isUpdated ()
        {
            return (updated);
        }

    }

    public class RateLimitStatus
    {
        protected RateLimitStatusItem realtime;
        protected RateLimitStatusItem general;

        protected RateLimitStatus ()
        {
        }

        protected RateLimitStatus (RateLimitStatus other)
        {
            realtime = new RateLimitStatusItem(other.realtime);
            general = new RateLimitStatusItem(other.general);
        }

        public String toString ()
        {
            return ("{general=" + general + ", realtime=" + realtime + "}");
        }

        /**
         * Status of the concurrent and sequential rate limits for realtime
         * queries.
         * 
         * @return
         */
        public RateLimitStatusItem getRealtime ()
        {
            return realtime;
        }

        /**
         * Status of the concurrent and sequential rate limits for general (i.e.
         * not real time) queries.
         * 
         * @return
         */
        public RateLimitStatusItem getGeneral ()
        {
            return general;
        }

    }

    protected void updateRateLimitStatus (Map<String, String> headers)
    {
        synchronized (rateLimitStatus) {
            updateStatusItem(headers, rateLimitStatus.realtime, "realtime");
            updateStatusItem(headers, rateLimitStatus.general, "");

            /*
             * it doesn't cost us much to check the number of runners every time
             * we get a new set of headers, and the benefit is that a running
             * customer program will adapt in real time, without having to be
             * restarted, when NetBase changes the user's rate limits.
             */
            checkRunnerThreads(realtimeRunnerThreads,
                               Math.min(rateLimitStatus.realtime.concurrent.max,
                                        MAX_REALTIME_THREADS),
                               realtimeQueue,
                               rateLimitStatus.realtime,
                               "realtime");

            checkRunnerThreads(generalRunnerThreads,
                               Math.min(rateLimitStatus.general.concurrent.max,
                                        MAX_GENERAL_THREADS),
                               generalQueue,
                               rateLimitStatus.general,
                               "general");
        }
    }

    protected void updateStatusItem (Map<String, String> headers,
                                     RateLimitStatusItem item,
                                     String tag)
    {
        RateLimitStatusFrame frame = getStatusFrame(headers, "x-" + tag
                + "ratelimit-");
        if (frame != null) {
            item.sequential = frame;
            item.updated = true;
        }
        frame = getStatusFrame(headers, "x-" + tag + "concurrentratelimit-");
        if (frame != null)
            item.concurrent = frame;

    }

    protected RateLimitStatusFrame getStatusFrame (Map<String, String> headers,
                                                   String prefix)
    {
        RateLimitStatusFrame frame = null;
        if (headers.containsKey(prefix + "max")) {
            frame = new RateLimitStatusFrame();
            frame.max = getHeaderValue(headers, prefix + "max");
            frame.remaining = getHeaderValue(headers, prefix + "remaining");
            frame.reset = getHeaderValue(headers, prefix + "reset");

            /*
             * the basic contract in the rate limit headers is that if there is
             * no count remaining, then we should wait the number of seconds in
             * the reset property before continuing.
             */
            if (frame.remaining <= 0)
                frame.waitUntilTime = System.currentTimeMillis() + frame.reset
                        * 1000L;
        }
        return (frame);
    }

    protected int getHeaderValue (Map<String, String> headers, String name)
    {
        String value = headers.get(name);
        return (Integer.parseInt(value));
    }

    // ====================================================
    // methods for running queries through the user channel
    // ====================================================

    /**
     * Run an InsightAPI Query and synchronously wait for it to complete. This
     * will block the current thread while the query makes its way to the top of
     * its queue, waits for the wait limit, and then runs.
     * 
     * @param query
     * @throws InterruptedException
     */
    public void run (InsightAPIQuery query)
        throws InterruptedException
    {
        start(query);
        query.waitForCompletion();
    }

    /**
     * Start an InsightAPI Query and return immediately. The query is added to
     * the UserChannel's queue of pending queries. When this query makes it to
     * the top of the queue, it will wait for the appropriate amount of time for
     * the rate limit, then run.
     * 
     * Application code may check query.isComplete() or run
     * query.waitForCompletion() in order to determine when the query has been
     * completely run.
     * 
     * @param query
     */
    public void start (InsightAPIQuery query)
    {
        query.setUrlBase(this.serverBaseURL + query.op);
        query.setParameter("user", user);
        query.setParameter("password", password);

        if (!query.mustRespectRateLimit)
            freebieQueue.add(query);

        else if (query.isRealtime())
            realtimeQueue.add(query);

        else
            generalQueue.add(query);
    }

    /**
     * Returns a static copy (one that won't be updated in the future) of the
     * user channel's RateLimitStatus structure. This can be used by the
     * application to access its rate limit. Values are only valid per queue
     * (general or realtime) after a query in that queue has been completed and
     * the internet headers from the result have been processed.
     * 
     * @return
     * @throws InterruptedException
     * @throws InsightAPIQueryException
     */
    public RateLimitStatus getRateLimitStatus ()
        throws InterruptedException,
            InsightAPIQueryException
    {
        synchronized (rateLimitStatus) {
            return (new RateLimitStatus(rateLimitStatus));
        }
    }

    // =======================================================================
    // QUERY RUNNER THREAD -- the UserChannel object keeps a pool of threads
    // that can run queries in each of the three categories, freebie, realtime
    // and general. The number of freebie threads is fixed; the others are
    // controlled by the concurrent.max properties in the respective headers.
    // =======================================================================

    protected class QueryRunnerThread extends Thread
    {
        protected LinkedBlockingQueue<InsightAPIQuery> queryQueue;
        protected RateLimitStatusItem rateLimitStatusItem;
        protected boolean running = true;
        protected String name;

        protected QueryRunnerThread (LinkedBlockingQueue<InsightAPIQuery> queryQueue,
                                     RateLimitStatusItem rateLimitStatusItem,
                                     String name)
        {
            this.queryQueue = queryQueue;
            this.rateLimitStatusItem = rateLimitStatusItem;
            this.name = name;
            setDaemon(true);
            this.setName(getClass().getSimpleName() + "-" + name + "-"
                    + getId());
        }

        public void run ()
        {
            if (debug)
                System.out.println(getName() + " starting");

            try {
                while (running) {

                    // pull a query from the queue
                    InsightAPIQuery query = queryQueue.take();

                    if (debug)
                        System.out.println(Thread.currentThread().getName()
                                + " running query " + query.serial);

                    // if its EOQ_QUERY, this is the way that the topside shuts
                    // down threads; quit.
                    if (query == EOQ_QUERY)
                        break;

                    // this while loop causes queries to be repeated if, in
                    // spite of our best efforts, we get a 403/rate limit
                    // exceeded from the server. This sometimes happens
                    // routinely, due to rounding in the information coming
                    // back from the server, and will also happen if more
                    // than one process is using the same server/user/pass
                    // combination at once.
                    while (true) {

                        if (query.mustRespectRateLimit) {

                            // this loop repeatedly checks the rate limit header
                            // values waiting for an acceptable time to run a
                            // query.
                            while (true) {
                                long waitUntilTime = 0L;

                                // get the earliest time that this query could
                                // run
                                synchronized (rateLimitStatus) {
                                    waitUntilTime = Math.max(rateLimitStatusItem.concurrent.waitUntilTime,
                                                             rateLimitStatusItem.sequential.waitUntilTime);
                                }

                                // determine how long to wait, and break if
                                // we're ready to go
                                long delay = waitUntilTime
                                        - System.currentTimeMillis();
                                if (delay <= 0)
                                    break;

                                // wait and repeat
                                logInfo(query.serial + " waiting " + delay
                                        + " ms for rate limit");
                                Thread.sleep(delay);
                            }
                        }

                        // now run the query.
                        query.runQuery(UserChannel.this);

                        // use the headers that came back from the server to
                        // update the rate limit status.
                        updateRateLimitStatus(query.getHeaders());

                        // and if the query is complete (and didn't get a
                        // 403/rate limit exceeded), break out of that loop
                        if (query.isComplete())
                            break;
                    }

                    // now parse the JSON result
                    query.parseResponseContent();

                    // all is well; wake up anybody who may be waiting for the
                    // query to finish.
                    synchronized (query) {
                        query.notifyAll();
                    }

                    if (debug)
                        System.out.println(Thread.currentThread().getName()
                                + " done with query " + query.serial);

                }
            }
            catch (Exception e) {
                e.printStackTrace();
            }

            if (debug)
                System.out.println(getName() + " done");
        }
    }

    protected void checkRunnerThreads (AtomicInteger runnerThreads,
                                       int numThreadsNeeded,
                                       LinkedBlockingQueue<InsightAPIQuery> queryQueue,
                                       RateLimitStatusItem statusItem,
                                       String name)
    {
        // if there are too many threads, send some EOQ_QUERYs which
        // will cause them to stop (eventually)
        while (runnerThreads.get() > numThreadsNeeded) {
            queryQueue.add(EOQ_QUERY);
            runnerThreads.getAndDecrement();
        }

        // if there are too few threads, start some more
        while (runnerThreads.get() < numThreadsNeeded) {
            QueryRunnerThread runnerThread = new QueryRunnerThread(queryQueue,
                                                                   statusItem,
                                                                   name);
            runnerThread.start();
            runnerThreads.getAndIncrement();
        }
    }

    /**
     * Shut down all the UserChannel's internal queue threads. This is not
     * permanent -- if subsequent run() or start() calls are made, the necessary
     * threads will simply be started up again.
     */
    public void close ()
        throws InterruptedException
    {
        synchronized (rateLimitStatus) {
            while (freebieRunnerThreads.decrementAndGet() >= 0) {
                freebieQueue.put(EOQ_QUERY);
            }

            while (realtimeRunnerThreads.decrementAndGet() >= 0) {
                realtimeQueue.put(EOQ_QUERY);
            }

            while (generalRunnerThreads.decrementAndGet() >= 0) {
                generalQueue.put(EOQ_QUERY);
            }
        }
    }

    /**
     * Write an "info" level log message. The user may override this method in
     * order to incorporate his favorite logging package.
     * 
     * @param text
     */
    public void logInfo (String text)
    {
        System.out.println(getClass().getSimpleName() + " log info: " + text);
    }

    /**
     * Write an "warning" level log message. The user may override this method
     * in order to incorporate her favorite logging package.
     * 
     * @param text
     */
    public void logWarning (String text)
    {
        System.err.println(getClass().getSimpleName() + " log warning: " + text);
    }

    /**
     * Returns the username provided to the NetBase API for authentication.
     * 
     * @return
     */
    public String getUser ()
    {
        return user;
    }

    /**
     * Returns the password provided to the NetBase API for authentication.
     * 
     * @return
     */
    public String getPassword ()
    {
        return password;
    }

    /**
     * Returns the URL at which the NetBase Insight API will be contacted. If
     * the user has not explicitly set this, the default value will be returned.
     * 
     * @return
     */
    public String getServerBaseURL ()
    {
        return serverBaseURL;
    }

    /**
     * Returns true if the UserChannel is logging debug messages.
     * 
     * @return
     */
    public boolean isDebug ()
    {
        return debug;
    }

    /**
     * Set the UserChannel to log (or not log) debug messages. Log messages will
     * use methods logInfo and logWarning, which may be overridden by user code
     * in order to incorporate a preferred logging system.
     * 
     * @param debug
     */
    public void setDebug (boolean debug)
    {
        this.debug = debug;
    }

}
