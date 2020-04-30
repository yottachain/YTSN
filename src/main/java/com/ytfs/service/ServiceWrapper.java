package com.ytfs.service;

import com.ytfs.service.check.QueryRebuildNode;
import com.ytfs.service.cost.NewObjectScanner;
import com.ytfs.service.cost.SumRelationShip;
import com.ytfs.service.cost.UserFeeStat;
import com.ytfs.service.servlet.bp.DNISenderPool;
import com.ytfs.service.servlet.bp.NodeStatSync;
import org.tanukisoftware.wrapper.WrapperListener;
import org.tanukisoftware.wrapper.WrapperManager;

public class ServiceWrapper implements WrapperListener {

    public static int REBUILDER_NODEID = 0;
    public static int REBUILDER_EXEC_NODEID = 0;
    public static boolean SPOTCHECK = false;
    public static int SPOTCHECKNUM = 3;
    public static boolean FEESUM = false;
    public static boolean DE_DUPLICATION = true;

    public static void main(String[] args) {
        WrapperManager.start(new ServiceWrapper(), args);
    }

    @Override
    public Integer start(String[] strings) {
        String de_duplication = WrapperManager.getProperties().getProperty("wrapper.ytsn.de_duplication", "on");
        if (de_duplication != null && de_duplication.trim().equalsIgnoreCase("off")) {
            DE_DUPLICATION = false;
        }
        String rebuild = WrapperManager.getProperties().getProperty("wrapper.ytsn.rebuild", "on");
        String spot = WrapperManager.getProperties().getProperty("wrapper.ytsn.spotcheck", "on");
        if (spot != null && spot.trim().equalsIgnoreCase("on")) {
            SPOTCHECK = true;
        }
        String spotnum = WrapperManager.getProperties().getProperty("wrapper.ytsn.spotcheck.num", "3");
        try {
            SPOTCHECKNUM = Integer.parseInt(spotnum);
            SPOTCHECKNUM = SPOTCHECKNUM < 3 ? 3 : SPOTCHECKNUM;
            SPOTCHECKNUM = SPOTCHECKNUM > 20 ? 20 : SPOTCHECKNUM;
        } catch (Throwable r) {
        }
        String feesum = WrapperManager.getProperties().getProperty("wrapper.ytsn.force.feesum", "off");
        if (feesum != null && feesum.trim().equalsIgnoreCase("on")) {
            FEESUM = true;
        }
        ServerInitor.init();
        NewObjectScanner.startUp();
        SumRelationShip.startUp();
        if (rebuild != null && rebuild.trim().equalsIgnoreCase("on")) {
            QueryRebuildNode.startUp();
        }
        UserFeeStat.startUp();
        DNISenderPool.startup();
        NodeStatSync.startup();
        return null;
    }

    @Override
    public int stop(int exitCode) {
        NewObjectScanner.shutdown();
        SumRelationShip.shutdown();
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
