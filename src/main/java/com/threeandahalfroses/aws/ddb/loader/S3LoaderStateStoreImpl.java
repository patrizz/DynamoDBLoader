package com.threeandahalfroses.aws.ddb.loader;

import com.threeandahalfroses.commons.aws.s3.S3Utility;
import com.threeandahalfroses.commons.general.JSONUtility;
import org.apache.commons.io.IOUtils;
import org.json.simple.JSONObject;
import org.json.simple.parser.ParseException;

import java.io.*;

/**
 * @author patrizz on 16/11/2016.
 * @copyright 2015 Three and a half Roses
 */
public class S3LoaderStateStoreImpl implements LoaderStateStore {

    public static final String ATTR_NAME = "name";
    public static final String ATTR_VARIABLES = "variables";
    private final String tempStateFilename;
    private final S3File s3File;

    S3LoaderStateStoreImpl(String tempStateFilename, S3File s3File) {
        this.tempStateFilename = tempStateFilename;
        this.s3File = s3File;
    }

    @Override
    public LoaderState loadLatest() {
        LoaderState loaderState = null;
        try {
            JSONObject jsonObject = (JSONObject) JSONUtility.toJSONAware(new FileReader(this.tempStateFilename));
            loaderState = new LoaderState(
                    LoaderState.StateName.valueOf((String) jsonObject.get(ATTR_NAME)),
                    (JSONObject) jsonObject.get(ATTR_VARIABLES)
            );
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return loaderState;
    }

    private String getJSONString(LoaderState loaderState) {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put(ATTR_NAME, loaderState.getStateName().name());
        jsonObject.put(ATTR_VARIABLES, loaderState.getStateVariables());
        return jsonObject.toJSONString();
    }

    @Override
    public void save(LoaderState loaderState) throws IOException {
        save(loaderState, false);
    }


    @Override
    public void save(LoaderState loaderState, boolean forceToPermanentStorage) throws IOException {
        String jsonString = getJSONString(loaderState);
        IOUtils.write(jsonString, new FileWriter(tempStateFilename));
        if (forceToPermanentStorage) {
            S3Utility.save(jsonString, s3File.getRegion(), s3File.getBucketName(), s3File.getKeyname(), null, null);
        }
    }

    @Override
    public void forceToPermanentStorage() throws IOException {
        String jsonString = IOUtils.toString(new FileReader(tempStateFilename));
        S3Utility.save(jsonString, s3File.getRegion(), s3File.getBucketName(), s3File.getKeyname(), null, null);
    }
}
