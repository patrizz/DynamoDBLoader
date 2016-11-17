package com.threeandahalfroses.aws.ddb.loader;

/**
 * @author patrizz on 16/11/2016.
 * @copyright 2015 Three and a half Roses
 */
public class FileLoaderStateSourceImpl implements LoaderStateSource {



    @Override
    public LoaderState loadLatest() {
        return null;
    }

    @Override
    public void save(LoaderState loaderState) {

    }

    @Override
    public void save(LoaderState loaderState, boolean forceToPermanentStorage) {

    }
}
