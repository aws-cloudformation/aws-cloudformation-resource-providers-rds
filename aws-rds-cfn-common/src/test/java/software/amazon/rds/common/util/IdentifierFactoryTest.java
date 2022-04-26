package software.amazon.rds.common.util;


import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

class IdentifierFactoryTest {

    private final static String DEFAULT_STACK_ID = "defaultStackId";
    private final static String DEFAULT_RESOURCE_ID = "defaultResourceId";
    private final static int DEFAULT_MAXLEN = 16;

    @Test
    public void test_defaultValues() {
        final IdentifierFactory factory = new IdentifierFactory(DEFAULT_STACK_ID, DEFAULT_RESOURCE_ID, DEFAULT_MAXLEN);
        final String identifier = factory.newIdentifier().toString();
        assertValidIdentifier(identifier, DEFAULT_MAXLEN);
    }

    @Test
    public void test_setValues() {
        final IdentifierFactory factory = new IdentifierFactory(DEFAULT_STACK_ID, DEFAULT_RESOURCE_ID, DEFAULT_MAXLEN);
        final String identifier = factory.newIdentifier()
                .withStackId("stack-id")
                .withResourceId("resource-id")
                .withRequestToken("request-token")
                .toString();
        assertValidIdentifier(identifier, DEFAULT_MAXLEN);
    }

    private void assertValidIdentifier(final String identifier, final int maxlen) {
        Assertions.assertThat(identifier).isNotBlank();
        Assertions.assertThat(identifier).doesNotContain("--");
        Assertions.assertThat(identifier).doesNotStartWith("-");
        Assertions.assertThat(identifier).doesNotEndWith("-");
        Assertions.assertThat(identifier).isLowerCase();
        Assertions.assertThat(identifier.length()).isLessThanOrEqualTo(maxlen);
    }
}
