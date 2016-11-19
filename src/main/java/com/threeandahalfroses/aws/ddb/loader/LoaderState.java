package com.threeandahalfroses.aws.ddb.loader;

import org.json.simple.JSONObject;

/**
 * @author patrizz on 16/11/2016.
 * @copyright 2015 Three and a half Roses
 */
public class LoaderState extends JSONObject {


    public static enum StateName {
        OFF,
        STARTING,
        DIFFS_DETERMINED_AND_SAVED,
        DELETED_ITEMS_REMOVED_FROM_DYNAMODB,
        DELETED_DIFF_FILL,
        PROCESSING_FILE,
        FILE_PROCESSED
    }

    private StateName stateName = StateName.OFF;
    private JSONObject stateVariables;

    public void changeState(StateName stateName) {
        changeState(stateName, new JSONObject());
    }
    public void changeState(StateName stateName, JSONObject stateVariables) {
        this.stateName = stateName;
        this.stateVariables = stateVariables;
    }


    public LoaderState(StateName stateName) {
        this(stateName, new JSONObject());
    }
    public LoaderState(StateName stateName, JSONObject stateVariables) {
        this.stateName = stateName;
        this.stateVariables = stateVariables;
    }

    public StateName getStateName() {
        return stateName;
    }

    public JSONObject getStateVariables() {
        return stateVariables;
    }



}
