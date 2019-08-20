package com.ytfs.service;

import com.ytfs.service.check.QueryRebuildNode;
import com.ytfs.service.check.SendSpotCheckTask;
import com.ytfs.service.servlet.ErrorNodeCache;
import com.ytfs.service.servlet.NewObjectScanner;
import org.tanukisoftware.wrapper.WrapperListener;
import org.tanukisoftware.wrapper.WrapperManager;

public class ServiceWrapper implements WrapperListener {

    public static void main(String[] args) {
        WrapperManager.start(new ServiceWrapper(), args);
    }

    @Override
    public Integer start(String[] strings) {
        ServerInitor.init();
        NewObjectScanner.startUp();
        ErrorNodeCache.startUp();
        QueryRebuildNode.startUp();
        SendSpotCheckTask.startUp();
        return null;
    }

    @Override
    public int stop(int exitCode) {
        NewObjectScanner.shutdown();
        ErrorNodeCache.shutdown();
        SendSpotCheckTask.shutdown();
        QueryRebuildNode.shutdown();
        ServerInitor.stop();
        return exitCode;
    }

    @Override
    public void controlEvent(int event) {
        if (WrapperManager.isControlledByNativeWrapper() == false) {
            if (event == WrapperManager.WRAPPER_CTRL_C_EVENT
                    || event == WrapperManager.WRAPPER_CTRL_CLOSE_EVENT
                    || event == WrapperManager.WRAPPER_CTRL_SHUTDOWN_EVENT) {
                WrapperManager.stop(0);
            }
        }
    }
}
