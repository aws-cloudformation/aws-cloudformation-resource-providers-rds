package software.amazon.rds.common.client;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

class RdsUserAgentProviderTest {

    @Test
    public void test_RdsUserAgentProvider_getUserAgentPrefix() {
        Assertions.assertThat(RdsUserAgentProvider.getUserAgentPrefix()).isEqualTo(RdsUserAgentProvider.SDK_CLIENT_USER_AGENT_PREFIX);
    }

    @Test
    public void test_RdsUserAgentProvider_getUserAgentSuffix() {
        Assertions.assertThat(RdsUserAgentProvider.getUserAgentSuffix()).isNotBlank();
    }
}
