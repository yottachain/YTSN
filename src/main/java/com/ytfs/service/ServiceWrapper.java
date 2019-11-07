package com.ytfs.service;

import com.ytfs.service.check.QueryRebuildNode;
import com.ytfs.service.check.SendSpotCheckTask;
import com.ytfs.service.dao.test.ShardInsertTest;
import com.ytfs.service.dao.test.ShardUpdateTest;
import com.ytfs.service.servlet.NewObjectScanner;
import com.ytfs.service.servlet.bp.DNISenderPool;
import com.ytfs.service.servlet.bp.NodeStatSync;
import org.tanukisoftware.wrapper.WrapperListener;
import org.tanukisoftware.wrapper.WrapperManager;

public class ServiceWrapper implements WrapperListener {

    public static void main(String[] args) {
        WrapperManager.start(new ServiceWrapper(), args);
    }

    @Override
    public Integer start(String[] strings) {
        ServerInitor.init();
        //ShardUpdateTest.start();
        //NewObjectScanner.startUp();
        QueryRebuildNode.startUp();
        DNISenderPool.start();
        NodeStatSync.startup();
        return null;
    }

    @Override
    public int stop(int exitCode) {
        NewObjectScanner.shutdown();
        //ShardUpdateTest.stop();
        QueryRebuildNode.shutdown();
        DNISenderPool.stop();
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
