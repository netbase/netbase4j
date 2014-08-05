
package com.netbase.insightapi.clientlib;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

public class InsightAPIQueryTest extends InsightAPITestCase
{
    @Test
    public void test1 ()
        throws Exception
    {
        // create the data structure to keep track of user, password
        // and status of the rat limits
        UserChannel user = this.getUserChannel();
        // create a sample insightCount operation, that must respect
        // the rate limit
        InsightAPIQuery q = new InsightAPIQuery("insightCount");
        q.setParameter("topicIds", getTopicId());
        q.setParameter("categories", "Likes");
        q.setDebug(true);
        // create a sample insightCount operation, that must respect
        // the rate limit, and uses the realtime index only
        InsightAPIQuery qr = new InsightAPIQuery("insightCount");
        qr.setParameter("topics", "GlobalWarming");
        qr.setParameter("categories", "Likes");
        qr.setRealtime(true);
        qr.setDebug(true);
        // create a sample helloWorld operation, that doesn't respect
        // the rate limit.
        InsightAPIQuery hwq = new InsightAPIQuery("helloWorld");
        hwq.setParameter("language", "Chinese");
        hwq.setDebug(true);
        // list of the queries being run (so we can wait for them all)
        List<InsightAPIQuery> queries = new ArrayList<InsightAPIQuery>();
        // start 9 operations running
        for (int i = 0; i < 3; i++) {
            InsightAPIQuery qq = new InsightAPIQuery(q);
            user.start(qq);
            queries.add(qq);
            InsightAPIQuery qqr = new InsightAPIQuery(qr);
            user.start(qqr);
            queries.add(qqr);
            InsightAPIQuery qwq = new InsightAPIQuery(hwq);
            user.start(qwq);
            queries.add(qwq);
        }
        // wait for them all to finish
        for (InsightAPIQuery query : queries)
            query.waitForCompletion();
        user.close();
        System.out.println("done!");
    }
}
