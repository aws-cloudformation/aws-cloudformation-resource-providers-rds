package software.amazon.rds.common.logging;

import java.io.IOException;

public interface LogMessage {

    void append(Object object) throws IOException;

    void append(String message, Object object) throws IOException;

    void append(Throwable throwable) throws IOException;
}
