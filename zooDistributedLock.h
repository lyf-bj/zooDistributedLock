#ifndef _ZOODISTRIBUTEDLOCK_H_
#define _ZOODISTRIBUTEDLOCK_H_

#include <string>
#include <atomic>
#include "zookeeper.h"

class CZooDistributedLock
{
public:
    CZooDistributedLock();
    virtual ~CZooDistributedLock();

public:
    // All in Function
    static void initZooAndCreateLockNode(const char* host,const char* rootpath,int recv_timeout = 30000,int flags = 0);

    // Get Version
    static const char* getZooKeeperVersion();

    // Get App State Master Or Slave Or Idle
    static inline const std::atomic<int>& getDistributedState() { return m_nDistributedState; }

    // Get app zoonode
    static inline const std::string& getDistributedAppNode() { return m_strSequenceNodeName; }

    // Get Zoo Handle State true:close false:running
    static inline const std::atomic<bool>& getZooHandleState() { return m_bZooHandleClosed; }

    // Get Distributed Root Name
    static inline const std::string& getZooDistributedRootName() { return m_strZooDistributedRootPath;}

    // Get Zoo Distributed Root Created State
    // 0 false;1 success; 2 alreadyExisting
    static inline const std::atomic<int>& getZooDistributedRootCreateState() { return m_nZooDistributedRootCreated; }

    // Delete Distributed Root Node async
    static void deleteDistributedRoot();

    // Delete Distributed Root Node sync
    static void deleteDistributedRootSync();

    // Close ZooKeeper Handle
    static void closeZooHandle();


    /*************callback****************************************/
protected:
    static void initZooWatch(zhandle_t* zh,int type,int state,const char* path,void* watcherCtx);
    static void createNodeWatch(int rc,const char* name,const void* data);
    static void createSubNodeWatch(int rc, const char* name, const void* data);
    static void subNodeGetWatch(zhandle_t* zh,int type,int state,const char* path,void* watcherCtx);
    static void dataCompletion(int rc,const struct Stat* stat,const void* data);
    static void deleteNodecompletion(int rc,const void* data);
    static void getChildrenCompletion(int rc,const struct String_vector* strings,const void* data);


private:
    CZooDistributedLock(const CZooDistributedLock& object);
    CZooDistributedLock& operator =(const CZooDistributedLock& object);

    static const char* state2String(int state);
    static const char* type2String(int state);
    static const char* rc2String(int rc);

    // Init Zookeeper
    static bool initZookeeper(const char* host, int recv_timeout = 30000, int flags = 0);

    // Create lock root
    // just root node  root directory
    static void createZooDistributedRoot(const char* rootpath);

    // Create app unique SEQUENTIAL node
    static void createZooLockSubNode();

private:
    // zhandle 
    static zhandle_t*           m_pZooHandle;                   // zookeeper handle
    static std::atomic<bool>    m_bZooHandleClosed;             // zookeeper handle closed True:Yes Closed;False Opened

    // root state
    static std::string          m_strZooDistributedRootPath;    // zookeeper distributedRoot
    static std::atomic<int>     m_nZooDistributedRootCreated;   // zookeeper distributedRoot Created 0 false;1 success; 2 alreadyExisting

    // node concerned
    static std::string          m_strSequenceNodeName;          // app unique node name
    static int                  m_nSequenceNodeNum;             // app unique node index
    static std::atomic<int>     m_nDistributedState;            // application master or slave 0:Idle;1:slave;2:Master
};

#endif // !_ZOODISTRIBUTEDLOCK_H_
