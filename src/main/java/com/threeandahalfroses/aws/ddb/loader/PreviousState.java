package com.threeandahalfroses.aws.ddb.loader;

import com.threeandahalfroses.commons.general.Utility;
import org.json.simple.JSONObject;

import java.text.ParseException;
import java.util.Date;

/**
 * @author patrizz on 13/11/2016.
 * @copyright 2015 Three and a half Roses
 */
public class PreviousState extends JSONObject {

    public PreviousState() {
        super(new JSONObject());
    }

    public PreviousState(JSONObject object) {
        super(object);
    }

    public void setCreateDate(Date date) {
        put("create-update", Utility.toString(date));
    }

    public Date getCreateDate() {
        try {
            return Utility.fromString(get("create-update").toString());
        } catch (ParseException e) {
            e.printStackTrace();
            return null;
        }
    }

    public void setLastUpdate(Date date) {
        put("last-update", Utility.toString(date));
    }

    public Date getLastUpdate() {
        try {
            return Utility.fromString(get("last-update").toString());
        } catch (ParseException e) {
            e.printStackTrace();
            return null;
        }
    }

    public void setLastRowProcessed(Long row) {
        put("last-row-processed", row);
    }

    public Long getLastRowProcessed() {
        return (Long) get("last-row-processed");
    }




}
