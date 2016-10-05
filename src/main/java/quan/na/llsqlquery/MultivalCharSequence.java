package quan.na.llsqlquery;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

/*
 * @author quan.na
 * @created 2016/06/27
 * 
 * This character sequence return different values for each thread.
 */
public class MultivalCharSequence implements CharSequence {
    private static Logger log = Logger.getLogger(MultivalCharSequence.class.getName());

    Map<Long, StringBuilder> builderMap = new HashMap<>();

    public StringBuilder getBuilder() {
        Long threadId = Thread.currentThread().getId();
        log.info(">>> Creating builder for thread " + threadId);
        builderMap.put(threadId, new StringBuilder());
        return builderMap.get(threadId);
    }

    public void remove() {
        Long threadId = Thread.currentThread().getId();
        log.info("<<< Destroying builder for thread " + threadId);
        builderMap.put(threadId, null);
    }

    @Override
    public int length() {
        Long threadId = Thread.currentThread().getId();
        if (null == builderMap.get(threadId))
            return 0;
        return builderMap.get(threadId).length();
    }

    @Override
    public char charAt(int index) {
        Long threadId = Thread.currentThread().getId();
        if (null == builderMap.get(threadId))
            return 0;
        return builderMap.get(threadId).charAt(0);
    }

    @Override
    public CharSequence subSequence(int start, int end) {
        Long threadId = Thread.currentThread().getId();
        if (null == builderMap.get(threadId))
            return "";
        return builderMap.get(threadId).subSequence(start, end);
    }

    @Override
    public String toString() {
        Long threadId = Thread.currentThread().getId();
        log.info("--- Getting value for thread " + threadId);
        if (null == builderMap.get(threadId))
            return "";
        return builderMap.get(threadId).toString();
    }
}
