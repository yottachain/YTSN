package com.ytfs.service;

import com.ytfs.service.servlet.NewObjectScanner;
import org.tanukisoftware.wrapper.WrapperListener;
import org.tanukisoftware.wrapper.WrapperManager;

public class ServiceWrapper implements WrapperListener {

    public static boolean isServer() {
        return newObjectScanner.isAlive();
    }

    static NewObjectScanner newObjectScanner = new NewObjectScanner();

    public static void main(String[] args) {
        WrapperManager.start(new ServiceWrapper(), args);
    }

    @Override
    public Integer start(String[] strings) {
        if (!newObjectScanner.isAlive()) {            
            newObjectScanner.start();
            ServerInitor.init();
        }
        return null;
    }

    @Override
    public int stop(int exitCode) {
        ServerInitor.stop();
        newObjectScanner.close();
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
