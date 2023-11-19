package software.amazon.rds.test.common.core;

import java.util.Map;

import com.google.common.collect.ImmutableMap;

public enum ServiceProvider {
    EC2("ec2"),
    IAM("iam"),
    KMS("kms"),
    RDS("rds"),
    SDK("sdk"),
    S3("s3"),
    MEDIAIMPORT("mediaimport"),
    ASM("secretsmanager"),
    REDSHIFT("redshift");

    private final String name;

    private ServiceProvider(final String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return this.name;
    }

    private static final Map<String, ServiceProvider> SERVICE_CLIENT_CLASSES = ImmutableMap.<String, ServiceProvider>builder()
            .put("Ec2Client", EC2)
            .put("IamClient", IAM)
            .put("KmsClient", KMS)
            .put("RdsClient", RDS)
            .put("SdkClient", SDK)
            .put("AsmClient", ASM)
            .put("S3Client", S3)
            .put("MediaimportClient", MEDIAIMPORT)
            .build();


    public static ServiceProvider fromString(final String s) {
        for (final ServiceProvider provider : ServiceProvider.values()) {
            if (provider.name.equals(s)) {
                return provider;
            }
        }
        throw new RuntimeException(String.format("Unknown service provider: \"%s\"", s));
    }

    public static ServiceProvider fromClientClass(final Class<?> clientClass) {
        final String className = clientClass.getSimpleName();
        if (!SERVICE_CLIENT_CLASSES.containsKey(className)) {
            throw new RuntimeException(String.format("Service provider mapping is missing for class \"%s\"", className));
        }
        return SERVICE_CLIENT_CLASSES.get(className);
    }
}
