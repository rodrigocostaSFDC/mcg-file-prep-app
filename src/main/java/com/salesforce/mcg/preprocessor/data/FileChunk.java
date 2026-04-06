package com.salesforce.mcg.preprocessor.data;

import com.opencsv.ICSVWriter;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

public record FileChunk(
        List<String[]> chunk,
        ICSVWriter writer,
        String fileRequestId,
        long chunkIndex,
        AtomicLong totalRows
) {

    public boolean isEmpty(){
        return Objects.isNull(chunk) || chunk.isEmpty();
    }
}
