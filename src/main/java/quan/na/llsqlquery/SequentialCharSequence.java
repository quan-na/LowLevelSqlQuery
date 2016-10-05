package quan.na.llsqlquery;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

/*
 * @author quan.na
 * @created 2016/06/27
 * 
 * This character sequence return different values for each thread.
 * Each time get() function  in called, the value is increased and inserted into template.
 * 
 */
public class SequentialCharSequence implements CharSequence {
    private static Logger log = Logger.getLogger(SequentialCharSequence.class.getName());

    public static interface StringGen {
        String getString(Long sequence);
    }
    Map<Long, Long> seqMap = new HashMap<>();
    Map<Long, StringGen> genMap = new HashMap<>();

    public void setGen(StringGen gen) {
        Long threadId = Thread.currentThread().getId();
        log.info(">>> Creating generator for thread " + threadId);
        seqMap.put(threadId, 0L);
        genMap.put(threadId, gen);
    }

    public void resetSeq() {
        Long threadId = Thread.currentThread().getId();
        log.info("-- Reseting generator for thread " + threadId);
        seqMap.put(threadId, 0L);
    }

    public void removeGen() {
        Long threadId = Thread.currentThread().getId();
        log.info("<<< Destroying generator for thread " + threadId);
        seqMap.put(threadId, null);
        genMap.put(threadId, null);
    }

    @Override
    public int length() {
        Long threadId = Thread.currentThread().getId();
        if (null == seqMap.get(threadId))
            return 0;
        return genMap.get(threadId).getString(seqMap.get(threadId)).length();
    }

    @Override
    public char charAt(int index) {
        Long threadId = Thread.currentThread().getId();
        if (null == seqMap.get(threadId))
            return 0;
        String result = genMap.get(threadId).getString(seqMap.get(threadId));
        if (index == result.length() - 1) {
            log.info("--- Increase sequence for thread " + threadId);
            seqMap.put(threadId, seqMap.get(threadId) + 1L);
        }
        return result.charAt(index);
    }

    @Override
    public CharSequence subSequence(int start, int end) {
        Long threadId = Thread.currentThread().getId();
        if (null == seqMap.get(threadId))
            return "";
        String result = genMap.get(threadId).getString(seqMap.get(threadId));
        if (end >= result.length()) {
            log.info("--- Increase sequence for thread " + threadId);
            seqMap.put(threadId, seqMap.get(threadId) + 1L);
        }
        return result.subSequence(start, end);
    }

    @Override
    public String toString() {
        Long threadId = Thread.currentThread().getId();
        if (null == seqMap.get(threadId))
            return "";
        String result = genMap.get(threadId).getString(seqMap.get(threadId));
        log.info("--- Increase sequence for thread " + threadId);
        seqMap.put(threadId, seqMap.get(threadId) + 1L);
        return result;
    }
}
