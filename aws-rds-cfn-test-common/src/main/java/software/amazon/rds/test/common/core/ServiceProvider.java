package software.amazon.rds.test.common.core;

import java.util.Map;

import com.google.common.collect.ImmutableMap;

public enum ServiceProvider {
    EC2("ec2"),
    IAM("iam"),
    KMS("kms"),
    RDS("rds"),
    SDK("sdk");

    private final String name;

    private ServiceProvider(final String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return this.name;
    }

    private static final Map<String, ServiceProvider> SERVICE_CLIENT_CLASSES = ImmutableMap.of(
            "Ec2Client", EC2,
            "IamClient", IAM,
            "KmsClient", KMS,
            "RdsClient", RDS,
            "SdkClient", SDK
    );

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
