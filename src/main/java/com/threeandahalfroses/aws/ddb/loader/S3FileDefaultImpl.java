package com.threeandahalfroses.aws.ddb.loader;

import com.amazonaws.regions.Region;

/**
 * @author patrizz on 10/11/2016.
 * @copyright 2015 Three and a half Roses
 */
public class S3FileDefaultImpl implements BaseDataLoader.S3File {

    private final Region region;
    private final String bucketName;
    private final String keyname;

    public S3FileDefaultImpl(Region region, String bucketName, String keyname) {
        this.region = region;
        this.bucketName = bucketName;
        this.keyname = keyname;
    }

    @Override
    public Region getRegion() {
        return this.region;
    }

    @Override
    public String getBucketName() {
        return this.bucketName;
    }

    @Override
    public String getKeyname() {
        return this.keyname;
    }
}
