
package com.netbase.insightapi.clientlib;

import org.junit.Assume;
import org.junit.Before;

public class InsightAPITestCase
{
    public static final String USERNAME_PROPERTY = "insightapi.username";
    public static final String PASSWORD_PROPERTY = "insightapi.password";
    public static final String TOPICID_PROPERTY = "insightapi.topicid";
    public static final String DEFAULT_TOPICID = "88";

    private static String username = null;
    private static String password = null;
    private static UserChannel userChannel = null;
    private static int topicId;

    public InsightAPITestCase ()
    {
        username = System.getProperty(USERNAME_PROPERTY);
        password = System.getProperty(PASSWORD_PROPERTY);
        topicId = Integer.parseInt(System.getProperty(TOPICID_PROPERTY,
                                                      DEFAULT_TOPICID));
        userChannel = new UserChannel(username, password);
    }

    @Before
    public void checkCredentials ()
    {
        if (username == null || password == null) {
            System.out.format("Test will be ignored because %s and %s are not set\n",
                              USERNAME_PROPERTY,
                              PASSWORD_PROPERTY);
            Assume.assumeTrue(false);
        }
    }

    public UserChannel getUserChannel ()
    {
        return userChannel;
    }

    public int getTopicId ()
    {
        return (topicId);
    }
}
