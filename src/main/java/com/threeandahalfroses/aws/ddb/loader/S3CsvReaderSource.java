package com.threeandahalfroses.aws.ddb.loader;

import com.amazonaws.regions.Region;
import com.threeandahalfroses.commons.aws.s3.S3Utility;

import java.io.IOException;
import java.io.Reader;

/**
 * @author patrizz on 11/11/2016.
 * @copyright 2015 Three and a half Roses
 */
public class S3CsvReaderSource implements CsvReaderSource {

    private final Region region;
    private final String bucketName;
    private final String keyname;
    private final Boolean firstLineContainsHeaders;

    S3CsvReaderSource(Region region, String bucketName, String keyname, Boolean firstLineContainsHeaders) {
        this.region = region;
        this.bucketName = bucketName;
        this.keyname = keyname;
        this.firstLineContainsHeaders = firstLineContainsHeaders;
    }

    @Override
    public boolean firstLineContainsHeaders() {
        return firstLineContainsHeaders;
    }

    @Override
    public Reader getReader() throws IOException {
        return S3Utility.getReader(region, bucketName, keyname);
    }
}
