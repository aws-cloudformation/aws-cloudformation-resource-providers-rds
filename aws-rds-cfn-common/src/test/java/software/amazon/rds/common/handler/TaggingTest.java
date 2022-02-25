package software.amazon.rds.common.handler;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;

import com.google.common.collect.ImmutableMap;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
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
import software.amazon.rds.common.error.ErrorRuleSet;
import software.amazon.rds.common.error.ErrorStatus;
import software.amazon.rds.common.error.IgnoreErrorStatus;
import software.amazon.rds.common.error.UnexpectedErrorStatus;
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

    @Test
    void test_TagSet_isEmpty() {
        final Tagging.TagSet tagSet = Tagging.TagSet.builder().build();
        assertThat(tagSet.isEmpty()).isTrue();
    }

    @Test
    void test_TagSet_isNotEmpty() {
        final Tagging.TagSet tagSet = Tagging.TagSet.builder()
                .stackTags(Collections.singleton(Tag.builder().build()))
                .build();
        assertThat(tagSet.isEmpty()).isFalse();
    }

    @Test
    void test_TagSet_emptySet() {
        final Tagging.TagSet tagSet = Tagging.TagSet.emptySet();
        assertThat(tagSet.isEmpty()).isTrue();
    }

    @Test
    void test_SoftFailErrorRuleSet_AwsServiceException_AccessDenied() {
        final Exception exception = AwsServiceException.builder()
                .awsErrorDetails(AwsErrorDetails.builder()
                        .errorCode("AccessDenied")
                        .build())
                .build();
        final ErrorRuleSet ruleSet = Tagging.SOFT_FAIL_TAG_ERROR_RULE_SET;
        final ErrorStatus status = ruleSet.handle(exception);
        assertThat(status).isInstanceOf(IgnoreErrorStatus.class);
    }

    @Test
    void test_SoftFailErrorRuleSet_AwsServiceException_OtherCode() {
        final Exception exception = AwsServiceException.builder()
                .awsErrorDetails(AwsErrorDetails.builder()
                        .errorCode("InternalFailure")
                        .build())
                .build();
        final ErrorRuleSet ruleSet = Tagging.SOFT_FAIL_TAG_ERROR_RULE_SET;
        final ErrorStatus status = ruleSet.handle(exception);
        assertThat(status).isInstanceOf(UnexpectedErrorStatus.class);
    }

    @Test
    void test_SoftFailErrorRuleSet_OtherException() {
        final Exception exception = new RuntimeException("test exception");
        final ErrorRuleSet ruleSet = Tagging.SOFT_FAIL_TAG_ERROR_RULE_SET;
        final ErrorStatus status = ruleSet.handle(exception);
        assertThat(status).isInstanceOf(UnexpectedErrorStatus.class);
    }

    @Test
    void test_translateTagsToSdk() {
        final Set<Tag> systemTags = Stream.of(
                Tag.builder().key("system-tag-key-1").value("system-tag-value-1").build(),
                Tag.builder().key("system-tag-key-2").value("system-tag-value-2").build()
        ).collect(Collectors.toSet());
        final Set<Tag> stackTags = Stream.of(
                Tag.builder().key("stack-tag-key-1").value("stack-tag-value-1").build(),
                Tag.builder().key("stack-tag-key-2").value("stack-tag-value-2").build()
        ).collect(Collectors.toSet());
        final Set<Tag> resourceTags = Stream.of(
                Tag.builder().key("resource-tag-key-1").value("resource-tag-value-1").build(),
                Tag.builder().key("resource-tag-key-2").value("resource-tag-value-2").build()
        ).collect(Collectors.toSet());

        final Tagging.TagSet tagSet = Tagging.TagSet.builder()
                .systemTags(systemTags)
                .stackTags(stackTags)
                .resourceTags(resourceTags)
                .build();

        final Set<Tag> allTags = Tagging.translateTagsToSdk(tagSet);

        assertThat(allTags.containsAll(systemTags)).isTrue();
        assertThat(allTags.containsAll(stackTags)).isTrue();
        assertThat(allTags.containsAll(resourceTags)).isTrue();
    }

    @Test
    void test_exclude() {
        final Tagging.TagSet minuend = Tagging.TagSet.builder()
                .systemTags(Stream.of(
                        Tag.builder().key("system-tag-key-1").value("system-tag-value-1").build(),
                        Tag.builder().key("system-tag-key-2").value("system-tag-value-2").build()
                ).collect(Collectors.toSet()))
                .stackTags(Stream.of(
                        Tag.builder().key("stack-tag-key-1").value("stack-tag-value-1").build(),
                        Tag.builder().key("stack-tag-key-2").value("stack-tag-value-2").build()
                ).collect(Collectors.toSet()))
                .resourceTags(Stream.of(
                        Tag.builder().key("resource-tag-key-1").value("resource-tag-value-1").build(),
                        Tag.builder().key("resource-tag-key-2").value("resource-tag-value-2").build()
                ).collect(Collectors.toSet()))
                .build();

        final Tagging.TagSet subtrahend = Tagging.TagSet.builder()
                .systemTags(Stream.of(
                        Tag.builder().key("system-tag-key-1").value("system-tag-value-1").build()
                ).collect(Collectors.toSet()))
                .stackTags(Stream.of(
                        Tag.builder().key("stack-tag-key-1").value("stack-tag-value-1").build()
                ).collect(Collectors.toSet()))
                .resourceTags(Stream.of(
                        Tag.builder().key("resource-tag-key-1").value("resource-tag-value-1").build()
                ).collect(Collectors.toSet()))
                .build();

        final Tagging.TagSet expect = Tagging.TagSet.builder()
                .systemTags(Stream.of(
                        Tag.builder().key("system-tag-key-2").value("system-tag-value-2").build()
                ).collect(Collectors.toSet()))
                .stackTags(Stream.of(
                        Tag.builder().key("stack-tag-key-2").value("stack-tag-value-2").build()
                ).collect(Collectors.toSet()))
                .resourceTags(Stream.of(
                        Tag.builder().key("resource-tag-key-2").value("resource-tag-value-2").build()
                ).collect(Collectors.toSet()))
                .build();

        final Tagging.TagSet difference = Tagging.exclude(minuend, subtrahend);
        assertThat(difference).isEqualTo(expect);
    }

    @Test
    void test_bestEffortErrorRuleSet_emptyResourceTags() {
        final ErrorRuleSet errorRuleSet = Tagging.bestEffortErrorRuleSet(
                Tagging.TagSet.builder()
                        .stackTags(Collections.singleton(Tag.builder().build()))
                        .stackTags(Collections.singleton(Tag.builder().build()))
                        .build(),
                Tagging.TagSet.builder()
                        .stackTags(Collections.singleton(Tag.builder().build()))
                        .stackTags(Collections.singleton(Tag.builder().build()))
                        .build()
        );

        assertThat(errorRuleSet).isEqualTo(Tagging.SOFT_FAIL_TAG_ERROR_RULE_SET);
    }

    @Test
    void test_bestEffortErrorRuleSet_nonEmptyResourceTags() {
        assertThat(Tagging.bestEffortErrorRuleSet(
                Tagging.TagSet.builder()
                        .stackTags(Collections.singleton(Tag.builder().build()))
                        .stackTags(Collections.singleton(Tag.builder().build()))
                        .resourceTags(Collections.singleton(Tag.builder().build()))
                        .build(),
                Tagging.TagSet.builder()
                        .stackTags(Collections.singleton(Tag.builder().build()))
                        .stackTags(Collections.singleton(Tag.builder().build()))
                        .resourceTags(Collections.singleton(Tag.builder().build()))
                        .build()
        )).isEqualTo(Tagging.HARD_FAIL_TAG_ERROR_RULE_SET);

        assertThat(Tagging.bestEffortErrorRuleSet(
                Tagging.TagSet.builder()
                        .stackTags(Collections.singleton(Tag.builder().build()))
                        .stackTags(Collections.singleton(Tag.builder().build()))
                        .resourceTags(Collections.emptySet())
                        .build(),
                Tagging.TagSet.builder()
                        .stackTags(Collections.singleton(Tag.builder().build()))
                        .stackTags(Collections.singleton(Tag.builder().build()))
                        .resourceTags(Collections.singleton(Tag.builder().build()))
                        .build()
        )).isEqualTo(Tagging.HARD_FAIL_TAG_ERROR_RULE_SET);

        assertThat(Tagging.bestEffortErrorRuleSet(
                Tagging.TagSet.builder()
                        .stackTags(Collections.singleton(Tag.builder().build()))
                        .stackTags(Collections.singleton(Tag.builder().build()))
                        .resourceTags(Collections.singleton(Tag.builder().build()))
                        .build(),
                Tagging.TagSet.builder()
                        .stackTags(Collections.singleton(Tag.builder().build()))
                        .stackTags(Collections.singleton(Tag.builder().build()))
                        .resourceTags(Collections.emptySet())
                        .build()
        )).isEqualTo(Tagging.HARD_FAIL_TAG_ERROR_RULE_SET);
    }
}
