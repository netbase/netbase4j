
package com.netbase.insightapi.bestpractice;

import org.json.simple.JSONArray;
import org.junit.Test;

import com.netbase.insightapi.bestpractice.TopicDownloader.ResultHandler;
import com.netbase.insightapi.clientlib.InsightAPIQuery;
import com.netbase.insightapi.clientlib.InsightAPITestCase;
import com.netbase.insightapi.clientlib.UserChannel;

public class TopicDownloaderTest extends InsightAPITestCase implements
                                                           ResultHandler
{
    @Test
    public void test1 ()
        throws Exception
    {
        UserChannel user = this.getUserChannel();
        InsightAPIQuery query = new InsightAPIQuery("retrieveDocuments");
        query.setParameter("topicIds", getTopicId());
        query.setParameter("sizeNeeded", 1997);
        query.setTimeout(1000000);
        query.setDebug(true);
        /*
         * divide the downloading job into 10 "chunks" and run them all at the
         * same time.
         */
        TopicDownloader.downloadHistory(query,
                                        user,
                                        Integer.MIN_VALUE,
                                        Integer.MAX_VALUE,
                                        this,
                                        10);
    }

    public void handleResult (JSONArray results, int first)
    {
        int t1 = TopicDownloader.getDocTimestamp(results.get(first));
        int t2 = TopicDownloader.getDocTimestamp(results.get(results.size() - 1));
        System.out.println(Thread.currentThread().getName() + " "
                + results.size() + " docs, " + first + " dups, t1=" + t1
                + ", t2=" + t2);
    }
}
