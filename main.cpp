#include <iostream>
#include <thread>
#include <csignal>
#include "LogManager.h"
#include "zooDistributedLock.h"

static bool bMonitorLoop = true;

void signalProcHandler(int signal_num)
{
    static int repeat_num = 0;
    switch(signal_num)
    {
    case SIGINT:        // CTRL+C signal
        std::cout << "CTRL+C signal:" << signal_num << std::endl;
        break;
    case SIGILL:        // Illegal instruction
        std::cout << "Illegal instruction:" << signal_num << std::endl;
        break;
    case SIGFPE:        // Floating-point error
        std::cout << "Floating-point error:" << signal_num << std::endl;
        break;
    case SIGSEGV:       // Illegal storage access
        std::cout << "Illegal storage access:" << signal_num << std::endl;
        break;
    case SIGTERM:       // Termination request
        std::cout << "Termination request:" << signal_num << std::endl;
        break;
    case SIGABRT:       // Abnormal termination
        std::cout << "Abnormal termination:" << signal_num << std::endl;
        break;
    default:
        std::cout << "Other signal ID:" << signal_num << std::endl;
        break;
    }

    repeat_num++;
    if(repeat_num > 0)
    {
        //CZooDistributedLock::deleteDistributedRoot();
        CZooDistributedLock::deleteDistributedRootSync();
        std::this_thread::sleep_for(std::chrono::milliseconds(3000));
        CZooDistributedLock::closeZooHandle();
        bMonitorLoop = false;
    }
}

void printZooState()
{
    static int index = 0;
    while(CZooDistributedLock::getZooHandleState())
    {
        std::cout << "ZooKeeper not Connected(1ms Loop)\n";
        CLogManager::getSingleton().AddLog(INFO_LOG, "ZooKeeper not Connected(1ms Loop)");

        std::cout << "Distributed Root Node:" << CZooDistributedLock::getZooDistributedRootName() << std::endl;
        std::cout << "Distributed Root State:" << CZooDistributedLock::getZooDistributedRootCreateState() << std::endl;
        std::cout << "Distributed Root SubNode:" << CZooDistributedLock::getDistributedAppNode() << std::endl;
        std::cout << "SubNode AppState:" << CZooDistributedLock::getDistributedState() << std::endl;
        
        CLogManager::getSingleton().AddLog(INFO_LOG, "Distributed Root Node:%s", CZooDistributedLock::getZooDistributedRootName().c_str());
        CLogManager::getSingleton().AddLog(INFO_LOG, "Distributed Root State:%d",(int)CZooDistributedLock::getZooDistributedRootCreateState());
        CLogManager::getSingleton().AddLog(INFO_LOG, "Distributed Root SubNode:%s", CZooDistributedLock::getDistributedAppNode().c_str());
        CLogManager::getSingleton().AddLog(INFO_LOG, "SubNode AppState:%d", (int)CZooDistributedLock::getDistributedState());

        index++;
        if(index > 5000)
        {
            std::cout << "5s zooKeeper not connected.Close Thread.\n";
            CLogManager::getSingleton().AddLog(INFO_LOG, "5s zooKeeper not connected.Close Thread.");
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    while(!CZooDistributedLock::getZooHandleState())
    {
        if (index > 0)
            index = 0;
        std::cout << "ZooKeeper Connected(1s Loop)\n";
        CLogManager::getSingleton().AddLog(INFO_LOG, "ZooKeeper Connected(1s Loop)");

        std::cout << "Distributed Root Node:" << CZooDistributedLock::getZooDistributedRootName() << std::endl;
        std::cout << "Distributed Root State:" << CZooDistributedLock::getZooDistributedRootCreateState() << std::endl;
        std::cout << "Distributed Root SubNode:" << CZooDistributedLock::getDistributedAppNode() << std::endl;
        std::cout << "SubNode AppState:" << CZooDistributedLock::getDistributedState() << std::endl;

        CLogManager::getSingleton().AddLog(INFO_LOG, "Distributed Root Node:%s", CZooDistributedLock::getZooDistributedRootName().c_str());
        CLogManager::getSingleton().AddLog(INFO_LOG, "Distributed Root State:%d", (int)CZooDistributedLock::getZooDistributedRootCreateState());
        CLogManager::getSingleton().AddLog(INFO_LOG, "Distributed Root SubNode:%s", CZooDistributedLock::getDistributedAppNode().c_str());
        CLogManager::getSingleton().AddLog(INFO_LOG, "SubNode AppState:%d", (int)CZooDistributedLock::getDistributedState());


        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }
}
int main(int argc,char** argv)
{
    if(argc != 3)
    {
        std::cout << "Exam: **** host DistributedRootPath\n";
        return 0;
    }

    CLogManager* m_pLogManager = new CLogManager();

    std::cout << "zooDistributedLock Test\n\n";
    std::cout << "getZooKeeperVersion:" << CZooDistributedLock::getZooKeeperVersion() << std::endl;
    char* pHost = argv[1];  // "127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183"
    char* pRootPath = argv[2];  // "/zooMonitor" 
    std::cout << "Host:" << pHost << std::endl;
    std::cout << "DistributedPath:" << pRootPath << std::endl;
    CZooDistributedLock::initZooAndCreateLockNode(pHost,pRootPath);

    std::thread st(printZooState);
    st.detach();

    signal(SIGINT, signalProcHandler);
    signal(SIGILL, signalProcHandler);
    signal(SIGFPE, signalProcHandler);
    signal(SIGSEGV, signalProcHandler);
    signal(SIGTERM, signalProcHandler);
    signal(SIGABRT, signalProcHandler);

    while(bMonitorLoop)
        ;

    
    std::cout << "\n\nTest Over\n\n";
    //std::cout << "Sleep 3s";
    //std::this_thread::sleep_for(std::chrono::milliseconds(3000));
    //std::cin.get();
    system("pause");
    delete(m_pLogManager);

    return 0;
}