package software.amazon.rds.dbinstance.client;

import java.util.function.Supplier;

import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.cloudformation.LambdaWrapper;

public class Ec2ClientBuilder {

    public static final Supplier<SdkHttpClient> LAMBDA_HTTP_CLIENT_SUPPLIER = () -> LambdaWrapper.HTTP_CLIENT;

    public final Supplier<SdkHttpClient> httpClientSupplier;

    public Ec2ClientBuilder() {
        this(LAMBDA_HTTP_CLIENT_SUPPLIER);
    }

    public Ec2ClientBuilder(final Supplier<SdkHttpClient> httpClientSupplier) {
        super();
        this.httpClientSupplier = httpClientSupplier;
    }

    public Ec2Client getClient() {
        return Ec2Client.builder()
                .httpClient(LambdaWrapper.HTTP_CLIENT)
                .build();
    }
}
