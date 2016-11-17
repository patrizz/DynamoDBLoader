package com.threeandahalfroses.aws.ddb.loader;

import com.amazonaws.regions.Region;

/**
 * @author patrizz on 16/11/2016.
 * @copyright 2015 Three and a half Roses
 */
public class S3File {
    private Region region;
    private String bucketName;
    private String keyname;

    public S3File(
            Region region,
            String bucketName,
            String keyname
    ) {
        this.region = region;
        this.bucketName = bucketName;
        this.keyname = keyname;
    }

    public Region getRegion() {
        return region;
    }
    public String getBucketName() {
        return bucketName;
    }
    public String getKeyname() {
        return keyname;
    }
}
