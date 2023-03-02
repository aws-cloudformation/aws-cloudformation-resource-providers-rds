package software.amazon.rds.common.handler;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.time.Instant;
import java.util.function.Predicate;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DescribeEventsRequest;
import software.amazon.awssdk.services.rds.model.DescribeEventsResponse;
import software.amazon.awssdk.services.rds.model.Event;
import software.amazon.awssdk.services.rds.model.ResourceNotFoundException;
import software.amazon.awssdk.services.rds.model.SourceType;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.common.logging.LoggingProxyClient;
import software.amazon.rds.common.logging.RequestLogger;
import software.amazon.rds.common.printer.FilteredJsonPrinter;

class EventsTest extends ProxyClientTestBase {
    @Mock
    RdsClient rds;

    @Mock
    private AmazonWebServicesClientProxy proxy;

    @Mock
    private ProxyClient<RdsClient> proxyRdsClient;

    @BeforeEach
    public void setup() {
        proxy = new AmazonWebServicesClientProxy(logger, MOCK_CREDENTIALS, () -> Duration.ofSeconds(600).toMillis());
        rds = mock(RdsClient.class);
        ResourceHandlerRequest<Void> request = new ResourceHandlerRequest<>();
        proxyRdsClient = new LoggingProxyClient<>(new RequestLogger(null, request, new FilteredJsonPrinter()), MOCK_PROXY(proxy, rds));
    }

    @Test
    public void test_checkFailedEvents_simple_success() {
        final ProgressEvent<Void, Void> progressEvent = new ProgressEvent<>();

        when(proxyRdsClient.client().describeEvents(any(DescribeEventsRequest.class))).thenReturn(DescribeEventsResponse.builder().build());

        Predicate<Event> isFailureEvent = event -> false;

        ProgressEvent<Void, Void> resultEvent = Events.checkFailedEvents(proxyRdsClient, logger, progressEvent, Instant.parse("2023-02-15T19:34:50Z"), "test_identifier", SourceType.DB_CLUSTER, isFailureEvent);
        assertThat(resultEvent).isNotNull();
        assertThat(resultEvent.isFailed()).isFalse();
    }

    @Test
    public void test_checkFailedEvents_simple_failure() {
        final ProgressEvent<Void, Void> progressEvent = new ProgressEvent<>();

        when(proxyRdsClient.client().describeEvents(any(DescribeEventsRequest.class))).thenReturn(DescribeEventsResponse.builder().events(Event.builder()
                .message("failed to create").build()).build());

        Predicate<Event> isFailureEvent = event -> Events.isEventMessageContains(event, "failed to update") ||
                Events.isEventMessageContains(null, "failed to update") ||
                Events.isEventMessageContains(event, "failed to create");

        ProgressEvent<Void, Void> resultEvent = Events.checkFailedEvents(proxyRdsClient, logger, progressEvent, Instant.parse("2023-02-15T19:34:50Z"), "test_identifier", SourceType.DB_CLUSTER, isFailureEvent);
        assertThat(resultEvent).isNotNull();
        assertThat(resultEvent.isFailed()).isTrue();
    }

    @Test
    public void test_checkFailedEvents_exception_failure() {
        final ProgressEvent<Void, Void> progressEvent = new ProgressEvent<>();

        when(proxyRdsClient.client().describeEvents(any(DescribeEventsRequest.class))).thenThrow(ResourceNotFoundException.builder().build());

        Predicate<Event> isFailureEvent = event -> true;

        ProgressEvent<Void, Void> resultEvent = Events.checkFailedEvents(proxyRdsClient, logger, progressEvent, Instant.parse("2023-02-15T19:34:50Z"), "test_identifier", SourceType.DB_CLUSTER, isFailureEvent);
        assertThat(resultEvent).isNotNull();
        assertThat(resultEvent.isFailed()).isTrue();
    }
}
