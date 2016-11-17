package com.threeandahalfroses.aws.ddb.loader;

import com.threeandahalfroses.commons.general.JSONUtility;
import org.apache.commons.io.IOUtils;
import org.json.simple.JSONObject;
import org.json.simple.parser.ParseException;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;

/**
 * @author patrizz on 16/11/2016.
 * @copyright 2015 Three and a half Roses
 */
public class S3LoaderStateImpl implements LoaderStateSource {

    public static final String ATTR_NAME = "name";
    public static final String ATTR_VARIABLES = "variables";
    private final String tempStateFilename;
    private final S3File s3File;

    S3LoaderStateImpl(String tempStateFilename, S3File s3File) {
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

    @Override
    public void save(LoaderState loaderState) {

    }

    @Override
    public void save(LoaderState loaderState, boolean forceToPermanentStorage) {

    }

    @Override
    public void forceToPermanentStorage() {

    }
}
