package software.amazon.rds.dbsubnetgroup;

import software.amazon.awssdk.services.rds.RdsClient;

public class ClientBuilder {
    private static class LazyHolder { static final RdsClient RDS_CLIENT = RdsClient.create();}
    public static RdsClient getClient() {
        return LazyHolder.RDS_CLIENT;
    }
}
