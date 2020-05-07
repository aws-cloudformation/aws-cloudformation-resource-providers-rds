package software.amazon.rds.dbclusterparametergroup;


import org.mockito.internal.util.collections.Sets;
import software.amazon.cloudformation.proxy.Credentials;
import software.amazon.cloudformation.proxy.LoggerProxy;

import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class AbstractTestBase {

    protected static final Credentials MOCK_CREDENTIALS;
    protected static final org.slf4j.Logger delegate;
    protected static final LoggerProxy logger;


    protected static final String DESCRIPTION;
    protected static final String FAMILY;
    protected static final Set<Tag> TAG_SET;


    static {
        System.setProperty("org.slf4j.simpleLogger.showDateTime", "true");
        System.setProperty("org.slf4j.simpleLogger.dateTimeFormat", "HH:mm:ss:SSS Z");
        MOCK_CREDENTIALS = new Credentials("accessKey", "secretKey", "token");

        delegate = LoggerFactory.getLogger("testing");
        logger = new LoggerProxy();

        DESCRIPTION = "sample description";
        FAMILY = "default.aurora.5";
        TAG_SET = Sets.newSet(Tag.builder().key("key").value("value").build());
    }

    static Map<String, String> translateTagsToMap(final Set<Tag> tags) {
        return tags.stream()
                .collect(Collectors.toMap(Tag::getKey, Tag::getValue));

    }
}
