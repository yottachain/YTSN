package com.ytfs.service;

import org.tanukisoftware.wrapper.WrapperListener;
import org.tanukisoftware.wrapper.WrapperManager;

public class ServiceWrapper implements WrapperListener {

    public static boolean isServer() {
        return t.isAlive();
    }

    static Thread t = new Thread() {
        @Override
        public void run() {
            while (true) {
                try {
                    Thread.sleep(1000 * 60);
                } catch (InterruptedException ex) {
                    break;
                }
            }
        }
    };

    public static void main(String[] args) {
        WrapperManager.start(new ServiceWrapper(), args);
    }

    @Override
    public Integer start(String[] strings) {
        if (!t.isAlive()) {
            ServerInitor.init();
            t.start();
        }
        return null;
    }

    @Override
    public int stop(int exitCode) {
        ServerInitor.stop();
        t.interrupt();
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
