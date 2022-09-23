package software.amazon.rds.common.client;


import java.net.URI;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfSystemProperties;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.RdsClientBuilder;
import software.amazon.cloudformation.LambdaWrapper;

class BaseSdkClientProviderTest {

    @Test
    public void test_BaseSdkClientProvider_emptyHttpSupplier() {
        final TestBaseSdkClientProvider provider = new TestBaseSdkClientProvider();
        final Supplier<SdkHttpClient> httpClientSupplier = provider.httpClientSupplier;
        Assertions.assertThat(httpClientSupplier.get()).isEqualTo(LambdaWrapper.HTTP_CLIENT);
    }

    @Test
    public void test_BaseSdkClientProvider_setHttpClient() {
        final TestBaseSdkClientProvider provider = new TestBaseSdkClientProvider();
        final RdsClientBuilder builderMock = Mockito.mock(RdsClientBuilder.class);
        Mockito.when(builderMock.httpClient(Mockito.any(SdkHttpClient.class)))
                .thenReturn(builderMock);

        provider.setHttpClient(builderMock);

        final ArgumentCaptor<SdkHttpClient> captor = ArgumentCaptor.forClass(SdkHttpClient.class);
        Mockito.verify(builderMock).httpClient(captor.capture());

        Assertions.assertThat(captor.getValue()).isEqualTo(LambdaWrapper.HTTP_CLIENT);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void test_BaseSdkClientProvider_setUserAgent() {
        final TestBaseSdkClientProvider provider = new TestBaseSdkClientProvider();

        RdsClientBuilder builderMock = Mockito.mock(RdsClientBuilder.class);
        Mockito.when(builderMock.overrideConfiguration(Mockito.any(Consumer.class)))
                .thenCallRealMethod();

        provider.setUserAgent(builderMock);
        Mockito.verify(builderMock).overrideConfiguration(Mockito.any(Consumer.class));
    }

    @Test
    @EnabledIfSystemProperty(named="rds.endpoint.override", matches = "^$")
     public void test_BaseSdkClientProvider_setEndpointOverride_byDefault() {
        final TestBaseSdkClientProvider provider = new TestBaseSdkClientProvider();

        RdsClientBuilder builderMock = Mockito.mock(RdsClientBuilder.class);

        provider.setEndpointOverride(builderMock);
        Mockito.verifyNoMoreInteractions(builderMock);
    }

    @Test
    @EnabledIfSystemProperty(named="rds.endpoint.override", matches = ".+")
    public void test_BaseSdkClientProvider_setEndpointOverride() {
        final TestBaseSdkClientProvider provider = new TestBaseSdkClientProvider();

        RdsClientBuilder builderMock = Mockito.mock(RdsClientBuilder.class);

        provider.setEndpointOverride(builderMock);
        Mockito.verify(builderMock).endpointOverride(Mockito.any(URI.class));
    }

    static class TestBaseSdkClientProvider extends BaseSdkClientProvider<RdsClientBuilder, RdsClient> {
        @Override
        public RdsClient getClient() {
            return RdsClient.builder().build();
        }
    }
}
