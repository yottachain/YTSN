package com.ytfs.service;

import com.ytfs.service.check.QueryRebuildNode;
import com.ytfs.service.cost.UserFeeStat;
import com.ytfs.service.servlet.bp.DNISenderPool;
import com.ytfs.service.servlet.bp.NodeStatSync;
import org.tanukisoftware.wrapper.WrapperListener;
import org.tanukisoftware.wrapper.WrapperManager;

public class ServiceWrapper implements WrapperListener {

    public static int REBUILDER_NODEID = 0;
    public static int REBUILDER_EXEC_NODEID = 0;

    public static void main(String[] args) {
        WrapperManager.start(new ServiceWrapper(), args);
    }

    @Override
    public Integer start(String[] strings) {
        ServerInitor.init();
        //NewObjectScanner.startUp();
        QueryRebuildNode.startUp();
        UserFeeStat.startUp();
        DNISenderPool.startup();
        NodeStatSync.startup();
        return null;
    }

    @Override
    public int stop(int exitCode) {
        //NewObjectScanner.shutdown();
        QueryRebuildNode.shutdown();
        UserFeeStat.shutdown();
        DNISenderPool.shutdown();
        NodeStatSync.terminate();
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
