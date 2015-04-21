package com.quantiply.rico.local;

import com.quantiply.rico.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ResourceBundle;
import java.io.FileReader;

import java.util.*;

public class LocalUtils {


    public static Map<String, String> getAll(String filePath) {

        Map<String, String> props = new HashMap<String, String>();

        try {
            FileReader fis = new FileReader(filePath);
            ResourceBundle bundle = new PropertyResourceBundle(fis);
            Enumeration<String> keys = bundle.getKeys();

            while (keys.hasMoreElements()) {
                String tmp = keys.nextElement();
                props.put(tmp, bundle.getString(tmp));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return props;
    }

}
