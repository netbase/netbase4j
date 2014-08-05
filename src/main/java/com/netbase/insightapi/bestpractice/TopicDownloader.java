
package com.netbase.insightapi.bestpractice;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import com.netbase.insightapi.clientlib.InsightAPIQuery;
import com.netbase.insightapi.clientlib.InsightAPIQuery.InsightAPIQueryException;
import com.netbase.insightapi.clientlib.UserChannel;
import com.netbase.insightapi.clientlib.UserChannel.RateLimitStatus;

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
 * Demonstrates NetBase's recommendation for an algorithm to download all the
 * documents of a topic, between two published date ranges or timestamp values
 * 
 */
public class TopicDownloader
{
    /**
     * A ResultHandler should iterate over the array of documents, starting with
     * index [first]. The documents from [0] through [first-1] are duplicates of
     * those received during the last call. First will be 0 on the first
     * iteration, and very often 1 afterward.
     */
    interface ResultHandler
    {
        void handleResult (JSONArray results, int first);
    }

    /**
     * Download topic history, using the filters, topic ids and other
     * specifications in the master query.
     * 
     * @param masterQuery
     * @param user
     * @param startTime
     *            inclusive
     * @param endTime
     *            exclusive
     * @param handler
     * @throws InterruptedException
     * @throws InsightAPIQueryException
     */
    public static void downloadHistory (InsightAPIQuery masterQuery,
                                        UserChannel user,
                                        long startTime,
                                        long endTime,
                                        ResultHandler handler)
        throws InterruptedException,
            InsightAPIQueryException
    {
        downloadHistory(masterQuery,
                        user,
                        InsightAPIQuery.longTimeToTimestamp(startTime),
                        InsightAPIQuery.longTimeToTimestamp(endTime),
                        handler);
    }

    /**
     * Download all the documents of a topic, in sequential order, by
     * publication date.
     * 
     * @param masterQuery
     * @param user
     * @param startTimestamp
     *            inclusive
     * @param endTimestamp
     *            exclusive
     * @param handler
     * @throws InterruptedException
     * @throws InsightAPIQueryException
     */
    public static void downloadHistory (InsightAPIQuery masterQuery,
                                        UserChannel user,
                                        int startTimestamp,
                                        int endTimestamp,
                                        ResultHandler handler)
        throws InterruptedException,
            InsightAPIQueryException
    {
        // clone the original query so we don't change it
        InsightAPIQuery query = new InsightAPIQuery(masterQuery);

        // force the parameters we rely on, leaving the others set by caller
        query.setParameter("sort", "timestamp");
        query.setOp("retrieveDocuments");

        // The caller can set "sizeNeeded" to any legal value, particularly for
        // testing purposes. In production, bigger is better unless we start
        // experiencing timeout or communication reliability issues.
        if (query.getParameters("sizeNeeded") == null)
            query.setParameter("sizeNeeded", 2000);

        /*
         * each call to the Insight API will return a (typically small) number
         * of documents that we already received in the prior call. This is
         * because we start the time range for call "n+1" with the highest
         * timestamp received during call "n". We do this because we are not
         * guaranteed to have received *all* of the documents containing the
         * highest timestamp.
         * 
         * Timestamp resolution is 1/10 second; so, typically, we'll receive
         * exactly one document at the end of call "n" and the beginning of
         * "n+1".
         * 
         * This set arranges for us to ignore the overlapped documents.
         */
        Set<String> docIdsAlreadySeen = new HashSet<String>();

        /*
         * the query for the first request covers the entire span for the
         * download. Since we're sorting and filtering by timestamp, we'll get
         * the earliest documents in the range.
         */
        query.setPublishedTimestampRange(startTimestamp, endTimestamp);

        while (true) {

            InsightAPIQuery q = new InsightAPIQuery(query);

            // run the query and toss an exception if it didn't work
            user.run(q);
            q.checkSuccess();

            // get the parsed json result
            JSONObject jsonResult = (JSONObject)q.getParsedContent();

            // get the array of documents
            JSONArray docs = (JSONArray)jsonResult.get("documents");

            // no documents at all? We're done
            if (docs == null || docs.size() == 0)
                break;

            // traverse the beginning of the list, counting up the duplicates
            int first = 0;
            while (first < docs.size()) {
                JSONObject doc = (JSONObject)docs.get(first);
                String docID = (String)getDocProperty(doc, "docID");
                if (!docIdsAlreadySeen.contains(docID))
                    break;
                first++;
            }

            // all duplicates? we're done.
            if (first >= docs.size())
                break;

            // call the ResultHandler to process the documents, beginning
            // with the first unique one
            handler.handleResult(docs, first);

            int last = docs.size() - 1;
            docIdsAlreadySeen.clear();

            // get the timestamp of the last document received
            int lastTimestamp = ((Number)getDocProperty(docs.get(last),
                                                        "timestamp")).intValue();

            // if it's later than (shouldn't be) or equal to (could be) the
            // end of the requested range, we're done
            if (lastTimestamp >= endTimestamp)
                break;

            /*
             * traverse backwards through the list from the end, looking for the
             * next-lower timestamp. Write down all the docIDs of these
             * documents, because we're going to see them again at the beginning
             * of the next query
             */
            while (last >= 0
                    && ((Number)getDocProperty(docs.get(last), "timestamp")).intValue() == lastTimestamp) {
                docIdsAlreadySeen.add((String)getDocProperty(docs.get(last),
                                                             "docID"));
                last--;
            }

            /*
             * If we get through this loop with last < 0, it means that the
             * entire block of documents we received had the same timestamp.
             * This is a failure of this algorithm.
             * 
             * For this to happen, it means that the topic contains more than
             * query.sizeNeeded (current max: 2000) documents with publication
             * timestamps in the same 1/10 second.
             * 
             * We have no choice but to increment the timestamp by 1/10 of a
             * second and move on. If we don't, we'll keep getting the same
             * result in an infinite loop.
             */
            if (last < 0) {
                user.logWarning(query.getSerial()
                        + " too many docs with same timestamp=" + lastTimestamp
                        + ", num of docs=" + docs.size());

                docIdsAlreadySeen.clear();
                lastTimestamp++;
            }

            // set the query's timestamp range to start with the last timestamp
            // we received, and rinse and repeat
            query.setPublishedTimestampRange(lastTimestamp, endTimestamp);
        }
    }

    protected static Object getDocProperty (Object d, String propName)
    {
        JSONObject doc = (JSONObject)d;
        JSONObject properties = (JSONObject)doc.get("properties");
        return (properties.get(propName));
    }

    protected static int getDocTimestamp (Object d)
    {
        return (((Number)getDocProperty(d, "timestamp")).intValue());
    }

    /**
     * Download all the documents in a topic, within a publication timestamp
     * range. Divide the work into "chunks" and perform each in parallel.
     * 
     * @param masterQuery
     * @param user
     * @param startTimestamp
     *            inclusive
     * @param endTimestamp
     *            exclusive
     * @param handler
     * @param numChunks
     * @throws InterruptedException
     * @throws InsightAPIQueryException
     */
    public static void downloadHistory (InsightAPIQuery masterQuery,
                                        UserChannel user,
                                        int startTimestamp,
                                        int endTimestamp,
                                        ResultHandler handler,
                                        int numChunks)
        throws InterruptedException,
            InsightAPIQueryException
    {
        // limit the number of chunks; no point on more of them than
        // we can run simultaneous queries.
        numChunks = Math.min(numChunks, UserChannel.MAX_GENERAL_THREADS);

        // get the actual earliest and latest timestamps; spending
        // the time to do this here ensures that the chunks are
        // allocated based on the real time span of the topic.
        startTimestamp = getEarliestTimestamp(masterQuery, user);
        endTimestamp = getLatestTimestamp(masterQuery, user);

        // now having made a couple queries, if the general rate limit
        // has been updated (which is probably the case), limit the
        // number of chunks by the maximum number of concurrent queries
        // allowed for general queries.
        RateLimitStatus status = user.getRateLimitStatus();
        if (status.getGeneral().isUpdated()) {
            numChunks = Math.min(numChunks, status.getGeneral()
                                                  .getConcurrent()
                                                  .getMax());
        }

        // each chunk will operate on "delta" timestamp units, except
        // for the last one, which will catch the roundoff error.
        int delta = (endTimestamp - startTimestamp) / numChunks;
        List<ChunkDownloaderThread> threads = new ArrayList<ChunkDownloaderThread>();

        // start each of the chunks running, allocating the timestamp
        // range sequentially
        for (int i = 0; i < numChunks; i++) {
            int chunkStartTimestamp = startTimestamp + delta * i;
            int chunkEndTimestamp = (i == numChunks - 1) ? endTimestamp
                    : chunkStartTimestamp + delta;
            ChunkDownloaderThread thread = new ChunkDownloaderThread(masterQuery,
                                                                     user,
                                                                     chunkStartTimestamp,
                                                                     chunkEndTimestamp,
                                                                     handler);
            threads.add(thread);
            thread.start();
        }

        // wait for all the chunks to finish
        for (Thread thread : threads)
            thread.join();
    }

    protected static class ChunkDownloaderThread extends Thread
    {
        InsightAPIQuery masterQuery;
        UserChannel user;
        int startTimestamp;
        int endTimestamp;
        ResultHandler handler;

        public ChunkDownloaderThread (InsightAPIQuery masterQuery,
                                      UserChannel user,
                                      int startTimestamp,
                                      int endTimestamp,
                                      ResultHandler handler)
        {
            this.masterQuery = masterQuery;
            this.user = user;
            this.startTimestamp = startTimestamp;
            this.endTimestamp = endTimestamp;
            this.handler = handler;
        }

        public void run ()
        {
            try {
                TopicDownloader.downloadHistory(masterQuery,
                                                user,
                                                startTimestamp,
                                                endTimestamp,
                                                handler);
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Gets the earliest timestamp that would match the query. Returns
     * Integer.MAX_VALUE if the query matches no documents.
     * 
     * @param masterQuery
     * @param user
     * @return
     * @throws InsightAPIQueryException
     * @throws InterruptedException
     */
    public static int getEarliestTimestamp (InsightAPIQuery masterQuery,
                                            UserChannel user)
        throws InsightAPIQueryException,
            InterruptedException
    {
        InsightAPIQuery query = new InsightAPIQuery(masterQuery);
        query.setOp("retrieveDocuments");
        query.setParameter("sort", "timestamp");
        query.setParameter("sizeNeeded", 1);
        user.run(query);
        JSONArray docs = (JSONArray)((JSONObject)query.getParsedContent()).get("documents");
        if (docs == null || docs.size() == 0)
            return (Integer.MAX_VALUE);
        JSONObject doc = (JSONObject)docs.get(0);
        return (getDocTimestamp(doc));
    }

    /**
     * gets the latest timestamp that would match the query. Returns
     * Integer.MIN_VALUE if the query matches no documents.
     * 
     * @param masterQuery
     * @param user
     * @return
     * @throws InsightAPIQueryException
     * @throws InterruptedException
     */
    public static int getLatestTimestamp (InsightAPIQuery masterQuery,
                                          UserChannel user)
        throws InsightAPIQueryException,
            InterruptedException
    {
        InsightAPIQuery query = new InsightAPIQuery(masterQuery);
        query.setOp("retrieveDocuments");
        query.setParameter("sort", "-timestamp");
        query.setParameter("sizeNeeded", 1);
        user.run(query);
        JSONArray docs = (JSONArray)((JSONObject)query.getParsedContent()).get("documents");

        if (docs == null || docs.size() == 0)
            return (Integer.MIN_VALUE);
        JSONObject doc = (JSONObject)docs.get(0);
        return (getDocTimestamp(doc));
    }

    /**
     * Demonstration main program. Arguments are provided as system properties,
     * using the "-D" syntax at the command line. This is intended as a
     * demonstration of the capability, rather than a useful utility in its own
     * right.
     */
    public static void main (String[] args)
    {
        try {
            // get the arguments from system properties
            String username = System.getProperty("username");
            String password = System.getProperty("password");
            String server = System.getProperty("server");
            String topicName = System.getProperty("topicName");
            String outputFile = System.getProperty("outputFile",
                                                   File.createTempFile("topic",
                                                                       ".json")
                                                       .getAbsolutePath());

            // create the user channel
            UserChannel user = new UserChannel(username, password, server);

            // create the InsightAPI query to be used as a prototype by the
            // downloader
            InsightAPIQuery query = new InsightAPIQuery("retrieveDocuments");
            query.setParameter("sizeNeeded", 500);
            query.setParameter("topics", topicName);

            // for demonstration purposes, extract only Tweets; other filters
            // can be added here.
            query.setParameter("domains", "twitter.com");

            // set a very large timeout. Large settings of "sizeNeeded" will
            // require this unless your network connection is very fast.
            query.setTimeout(1000000);

            // this causes log messages to describe what the downloader is
            // doing.
            query.setDebug(true);

            // create the result handler, which opens the output file
            MyResultHandler handler = new MyResultHandler(outputFile);

            // run the download
            System.out.println("downloading history of topic '" + topicName
                    + "' to file: " + outputFile);
            downloadHistory(query,
                            user,
                            Integer.MIN_VALUE,
                            Integer.MAX_VALUE,
                            handler);
            System.out.println("download of " + handler.numDocs + " complete");

            // finish up
            handler.close();

        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    protected static class MyResultHandler implements ResultHandler
    {
        PrintWriter writer;
        int numDocs = 0;

        MyResultHandler (String filename)
            throws IOException
        {
            writer = new PrintWriter(new FileWriter(filename));
        }

        public synchronized void handleResult (JSONArray results, int first)
        {
            int t1 = TopicDownloader.getDocTimestamp(results.get(first));
            int t2 = TopicDownloader.getDocTimestamp(results.get(results.size() - 1));
            System.out.println(Thread.currentThread().getName() + " "
                    + results.size() + " docs, " + first
                    + " dups, timestamp range: " + t1 + " to " + t2);

            try {
                for (int i = first; i < results.size(); i++) {
                    JSONObject doc = (JSONObject)results.get(i);
                    doc.writeJSONString(writer);
                    writer.write("\n");
                    numDocs++;
                }
                writer.flush();
            }
            catch (IOException ioe) {
                ioe.printStackTrace();
            }

        }

        public void close ()
        {
            writer.flush();
            writer.close();
        }
    }
}
