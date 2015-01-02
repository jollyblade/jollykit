package com.daftbyte.jollykit.process;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;

/**
 * @author szaboma
 */
public class ChunkProcessorTest extends ChunkProcessor<Long, String, BigDecimal> {

    /**
     * Sample usage of the Chunk Processor
     * @throws Exception
     */
    @Test
    public void testProcess() throws Exception {
        this.process();
    }

    @Override
    protected List<Long> getIds() {
        List<Long> numbers = new ArrayList<>();
        for(int i=0; i<100000; i++){
            numbers.add(Long.valueOf(i));
        }
        return  numbers;
    }

    @Override
    protected List<String> getResultsWithIds(List<Long> idLIst) {
        List<String> strings = new ArrayList<>();
        for (Long num : idLIst) {
            strings.add(num.toString());
        }
        return strings;
    }

    /**
     * Adding some error messages, they will be aggregated, and logged out at the end
     *
     * @param data
     * @return
     */
    @Override
    protected BigDecimal processRecord(String data) {
        if(data.equals("100") || data.equals("200")){
            errors.append("Could not process number!!!");
        }
        if (data.equals("1000") || data.equals("2000")) {
            errors.append("This is too big already!!!");
        }
        return new BigDecimal(data);
    }

    @Override
    protected void beforeChunk() {
        // Do nothing
    }

    @Override
    protected void afterChunk() {
        // commit for example, but for now, leave it blank
    }
}
