package software.amazon.rds.common.handler;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.time.Instant;
import java.util.function.Predicate;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;

import software.amazon.awssdk.core.exception.SdkServiceException;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DescribeEventsRequest;
import software.amazon.awssdk.services.rds.model.DescribeEventsResponse;
import software.amazon.awssdk.services.rds.model.Event;
import software.amazon.awssdk.services.rds.model.SourceType;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.common.logging.LoggingProxyClient;
import software.amazon.rds.common.logging.RequestLogger;
import software.amazon.rds.common.printer.FilteredJsonPrinter;

class EventsTest extends ProxyClientTestBase {
    public static final String FAILED_TO_CREATE_MESSAGE = "failed to create";
    public static final String SERVICE_INTERNAL_FAILURE_MESSAGE = "Service Internal Failure";
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
    public void test_checkFailedEvents_success() {
        final ProgressEvent<Void, Void> progressEvent = new ProgressEvent<>();

        when(proxyRdsClient.client().describeEvents(any(DescribeEventsRequest.class))).thenReturn(DescribeEventsResponse.builder().build());

        Predicate<Event> isFailureEvent = event -> false;

        Instant start = Instant.parse("2023-02-15T19:34:50Z");
        String identifier = "test_identifier";
        ProgressEvent<Void, Void> resultEvent = Events.checkFailedEvents(proxyRdsClient, identifier, SourceType.DB_CLUSTER, start, progressEvent, isFailureEvent, logger);

        assertThat(resultEvent).isNotNull();
        assertThat(resultEvent.isFailed()).isFalse();
        assertThat(resultEvent.getErrorCode()).isNull();

        ArgumentCaptor<DescribeEventsRequest> captor = ArgumentCaptor.forClass(DescribeEventsRequest.class);
        verify(proxyRdsClient.client(), atLeast(1)).describeEvents(captor.capture());
        assertThat(captor.getValue().startTime()).isEqualTo(start);
        assertThat(captor.getValue().sourceIdentifier()).isEqualTo(identifier);
        assertThat(captor.getValue().sourceType()).isEqualTo(SourceType.DB_CLUSTER);

    }

    @Test
    public void test_checkFailedEvents_failure() {
        final ProgressEvent<Void, Void> progressEvent = new ProgressEvent<>();

        when(proxyRdsClient.client().describeEvents(any(DescribeEventsRequest.class))).thenReturn(DescribeEventsResponse.builder().events(Event.builder()
                .message("failed to create").build()).build());

        Predicate<Event> isFailureEvent = event -> Events.isEventMessageContains(event, "failed to update") ||
                Events.isEventMessageContains(null, "failed to update") ||
                Events.isEventMessageContains(event, FAILED_TO_CREATE_MESSAGE);

        Instant start = Instant.parse("2023-02-15T19:34:50Z");
        String identifier = "test_identifier";
        ProgressEvent<Void, Void> resultEvent = Events.checkFailedEvents(proxyRdsClient, identifier, SourceType.DB_CLUSTER, start, progressEvent, isFailureEvent, logger);

        assertThat(resultEvent).isNotNull();
        assertThat(resultEvent.isFailed()).isTrue();
        assertThat(resultEvent.getErrorCode()).isEqualTo(HandlerErrorCode.GeneralServiceException);
        assertThat(resultEvent.getMessage()).isEqualTo(FAILED_TO_CREATE_MESSAGE);

        ArgumentCaptor<DescribeEventsRequest> captor = ArgumentCaptor.forClass(DescribeEventsRequest.class);
        verify(proxyRdsClient.client(), atLeast(1)).describeEvents(captor.capture());
        assertThat(captor.getValue().startTime()).isEqualTo(start);
        assertThat(captor.getValue().sourceIdentifier()).isEqualTo(identifier);
        assertThat(captor.getValue().sourceType()).isEqualTo(SourceType.DB_CLUSTER);

    }

    @Test
    public void test_checkFailedEvents_exception_failure() {
        final ProgressEvent<Void, Void> progressEvent = new ProgressEvent<>();

        when(proxyRdsClient.client().describeEvents(any(DescribeEventsRequest.class))).thenThrow(SdkServiceException.builder().message(SERVICE_INTERNAL_FAILURE_MESSAGE).build());

        Predicate<Event> isFailureEvent = event -> true;

        ProgressEvent<Void, Void> resultEvent = Events.checkFailedEvents(proxyRdsClient, "test_identifier", SourceType.DB_CLUSTER, Instant.parse("2023-02-15T19:34:50Z"), progressEvent, isFailureEvent, logger);
        assertThat(resultEvent).isNotNull();
        assertThat(resultEvent.isFailed()).isTrue();
        assertThat(resultEvent.getErrorCode()).isEqualTo(HandlerErrorCode.ServiceInternalError);
        assertThat(resultEvent.getMessage()).isEqualTo(SERVICE_INTERNAL_FAILURE_MESSAGE);
    }
}
