package software.amazon.rds.common.handler;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;

import com.google.common.collect.ImmutableMap;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.AddTagsToResourceRequest;
import software.amazon.awssdk.services.rds.model.AddTagsToResourceResponse;
import software.amazon.awssdk.services.rds.model.ListTagsForResourceRequest;
import software.amazon.awssdk.services.rds.model.ListTagsForResourceResponse;
import software.amazon.awssdk.services.rds.model.RemoveTagsFromResourceRequest;
import software.amazon.awssdk.services.rds.model.RemoveTagsFromResourceResponse;
import software.amazon.awssdk.services.rds.model.Tag;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.common.logging.LoggingProxyClient;
import software.amazon.rds.common.logging.RequestLogger;
import software.amazon.rds.common.printer.FilteredJsonPrinter;

public class TaggingTest extends ProxyClientTestBase {

    @Mock
    private AmazonWebServicesClientProxy proxy;

    @Mock
    private ProxyClient<RdsClient> proxyRdsClient;

    @Mock
    RdsClient rds;

    @BeforeEach
    public void setup() {
        proxy = new AmazonWebServicesClientProxy(logger, MOCK_CREDENTIALS, () -> Duration.ofSeconds(600).toMillis());
        rds = mock(RdsClient.class);
        ResourceHandlerRequest<Void> request = new ResourceHandlerRequest<>();
        proxyRdsClient = new LoggingProxyClient<>(new RequestLogger(null, request, new FilteredJsonPrinter()), MOCK_PROXY(proxy, rds));
    }

    @Test
    void simple_success() {
        final ProgressEvent<Void, Void> event = new ProgressEvent<>();
        Map<String, String> previousTags = ImmutableMap.of("key1", "value1", "key2", "value2");
        Map<String, String> desiredTags = ImmutableMap.of("key2", "value2", "key3", "value3");

        when(proxyRdsClient.client().addTagsToResource(any(AddTagsToResourceRequest.class))).thenReturn(AddTagsToResourceResponse.builder().build());
        when(proxyRdsClient.client().removeTagsFromResource(any(RemoveTagsFromResourceRequest.class))).thenReturn(RemoveTagsFromResourceResponse.builder().build());

        ProgressEvent<Void, Void> resultEvent = Tagging.updateTags(proxyRdsClient, "test-arn", event, previousTags, desiredTags, Commons.DEFAULT_ERROR_RULE_SET);
        assertThat(resultEvent).isNotNull();
        assertThat(resultEvent.isFailed()).isFalse();
    }

    @Test
    void empty_tags_to_add() {
        final ProgressEvent<Void, Void> event = new ProgressEvent<>();
        Map<String, String> previousTags = ImmutableMap.of("key1", "value1", "key2", "value2");

        ProgressEvent<Void, Void> resultEvent = Tagging.updateTags(proxyRdsClient, "test-arn", event, previousTags, previousTags, Commons.DEFAULT_ERROR_RULE_SET);
        assertThat(resultEvent).isNotNull();
        assertThat(resultEvent.isFailed()).isFalse();
    }

    @Test
    void simple_list_tags() {
        when(proxyRdsClient.client().listTagsForResource(any(ListTagsForResourceRequest.class)))
                .thenReturn(ListTagsForResourceResponse.builder()
                        .tagList(Tag.builder()
                                .key("key").value("value")
                                .build())
                        .build());
        Collection<Tag> result = Tagging.listTagsForResource(proxyRdsClient, "arn");
        assertThat(result).isNotEmpty();
    }
}
