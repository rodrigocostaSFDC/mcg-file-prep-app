package com.salesforce.mcg.preprocessor.data;

import java.util.List;

public record ColumnMapping(
        int phoneColIdx,
        List<Integer> urlColIdxs,
        int apiKeyColIdx,
        int templateNameColIdx,
        int subscriberKeyColIdx) {
}
