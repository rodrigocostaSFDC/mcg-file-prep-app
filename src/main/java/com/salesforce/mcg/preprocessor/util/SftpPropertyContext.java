package com.salesforce.mcg.preprocessor.util;

import com.salesforce.mcg.preprocessor.properties.SftpPreprocessorProperties;
import com.salesforce.mcg.preprocessor.properties.SftpServerProperties;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import static com.salesforce.mcg.preprocessor.common.AppConstants.*;

@Service
public class SftpPropertyContext {

    private final SftpPreprocessorProperties properties;
    private final String company;

    public SftpPropertyContext(
            SftpPreprocessorProperties properties,
            @Value("#{environment['company'] ?: 'telmex'}") String company){
        this.properties = properties;
        this.company = company;
    }

    public SftpServerProperties getPropertiesForActiveCompany(){
        return COMPANY_TELMEX.equals(company) ?
                    properties.telmex(): properties.telnor();
    }
}
