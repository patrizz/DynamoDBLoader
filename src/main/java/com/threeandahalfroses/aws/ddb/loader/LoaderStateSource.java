package com.threeandahalfroses.aws.ddb.loader;

/**
 * @author patrizz on 16/11/2016.
 * @copyright 2015 Three and a half Roses
 */
public interface LoaderStateSource {
    LoaderState loadLatest();
    void save(LoaderState loaderState);
    void save(LoaderState loaderState, boolean forceToPermanentStorage);
    void forceToPermanentStorage();
}
