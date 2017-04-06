#include "zooDistributedLock.h"
#include "LogManager.h"
#include <iostream>

zhandle_t* CZooDistributedLock::m_pZooHandle = nullptr;
std::atomic<bool> CZooDistributedLock::m_bZooHandleClosed = true;
std::string CZooDistributedLock::m_strZooDistributedRootPath = "";
std::atomic<int> CZooDistributedLock::m_nZooDistributedRootCreated = 0;
std::string CZooDistributedLock::m_strSequenceNodeName = "";
int CZooDistributedLock::m_nSequenceNodeNum = -1;
std::atomic<int> CZooDistributedLock::m_nDistributedState = 0;


CZooDistributedLock::CZooDistributedLock()
{
    CLogManager::getSingleton().AddLog(INFO_LOG, "%s", __FUNCTION__);
}

CZooDistributedLock::~CZooDistributedLock()
{
    CLogManager::getSingleton().AddLog(INFO_LOG, "%s", __FUNCTION__);
    if(m_pZooHandle != nullptr)
    {
        std::cout<<"Shutting down...\n";
        CLogManager::getSingleton().AddLog(ALL_LOG,"%s:Shutting down...", __FUNCTION__);
        zookeeper_close(m_pZooHandle);
        m_bZooHandleClosed = true;m_pZooHandle = nullptr;
        m_nDistributedState = 0;
    }
}

// Close ZooKeeper Handle
void CZooDistributedLock::closeZooHandle()
{
    CLogManager::getSingleton().AddLog(INFO_LOG, "%s", __FUNCTION__);
    if(m_pZooHandle != nullptr)
    {
        std::cout << "Shutting down...\n";
        CLogManager::getSingleton().AddLog(ALL_LOG, "%s:Shutting down...", __FUNCTION__);
        zookeeper_close(m_pZooHandle);
        m_bZooHandleClosed = true; m_pZooHandle = nullptr;
        m_nDistributedState = 0;
    }
}

// All in Function
void CZooDistributedLock::initZooAndCreateLockNode(const char* host, const char* rootpath, int recv_timeout, int flags)
{
    CLogManager::getSingleton().AddLog(INFO_LOG, "%s", __FUNCTION__);
    if(!initZookeeper(host,recv_timeout,flags))
    {
        std::cout << "initZookeeper:Host:" << host << ";Error\n";
        CLogManager::getSingleton().AddLog(ERROR_LOG, "%s:initZookeeper:Host:%s Error", __FUNCTION__,host);
        return;
    }

    createZooDistributedRoot(rootpath);
}

// Delete Distributed Root Node
// if Distributed Root has only this app node,then delete
void CZooDistributedLock::deleteDistributedRoot()
{
    CLogManager::getSingleton().AddLog(INFO_LOG, "%s", __FUNCTION__);
    if(m_nZooDistributedRootCreated != 0)
        zoo_aget_children(m_pZooHandle, m_strZooDistributedRootPath.c_str(),0,getChildrenCompletion,nullptr);
}

// Delete Distributed Root Node sync
void CZooDistributedLock::deleteDistributedRootSync()
{
    CLogManager::getSingleton().AddLog(INFO_LOG, "%s", __FUNCTION__);
    if(m_nZooDistributedRootCreated != 0)
    {
        struct String_vector strings;
        int rc = zoo_get_children(m_pZooHandle, m_strZooDistributedRootPath.c_str(), 0, &strings);
        CLogManager::getSingleton().AddLog(INFO_LOG, "%s:%s", __FUNCTION__, rc2String(rc));
        if(rc == ZOK)
        {
            if(strings.count > 0)
            {
                // Node children
                std::cout << "Node Children:" << m_strZooDistributedRootPath << "\n";
                CLogManager::getSingleton().AddLog(INFO_LOG, "%s:Node Children:%s", __FUNCTION__, m_strZooDistributedRootPath.c_str());
                for(int i = 0; i < strings.count; i++)
                {
                    std::cout << "\t" << strings.data[i] << "\n";
                    CLogManager::getSingleton().AddLog(INFO_LOG, "\t%s", strings.data[i]);
                }

                if(strings.count == 1)
                {
                    std::string strSubNode = m_strZooDistributedRootPath;
                    if (strSubNode.at(strSubNode.size() - 1) != '/')
                        strSubNode.append("/");
                    strSubNode.append(strings.data[0]);
                    rc = zoo_delete(m_pZooHandle, strSubNode.c_str(), -1);
                    rc = zoo_delete(m_pZooHandle, m_strZooDistributedRootPath.c_str(), -1);
                    if(rc == ZOK)
                    {
                        std::cout << "node:" << m_strZooDistributedRootPath << " delete success\n";
                        CLogManager::getSingleton().AddLog(INFO_LOG, "node:%s; delete success", m_strZooDistributedRootPath.c_str());
                        m_nZooDistributedRootCreated = 0;
                        m_nSequenceNodeNum = -1;
                        m_nDistributedState = 0;
                    }
                    else if(rc == ZNONODE)
                    {
                        std::cout << "node:" << m_strZooDistributedRootPath << " not exists.delete Failed\n";
                        CLogManager::getSingleton().AddLog(INFO_LOG, "node:%s; not exists.delete Failed", m_strZooDistributedRootPath.c_str());
                    }
                    else if(rc == ZNOAUTH)
                    {
                        std::cout << "node:" << m_strZooDistributedRootPath << ". the client does not have permission.delete Failed\n";
                        CLogManager::getSingleton().AddLog(INFO_LOG, "node:%s; the client does not have permission.delete Failed", m_strZooDistributedRootPath.c_str());
                    }
                    else if(rc == ZBADVERSION)
                    {
                        std::cout << "node:" << m_strZooDistributedRootPath << " expected version does not match actual version.delete Failed\n";
                        CLogManager::getSingleton().AddLog(INFO_LOG, "node:%s; expected version does not match actual version.delete Failed", m_strZooDistributedRootPath.c_str());
                    }
                    else if(rc == ZNOTEMPTY)
                    {
                        std::cout << "node:" << m_strZooDistributedRootPath << " children are present; node cannot be deleted.delete Failed\n";
                        CLogManager::getSingleton().AddLog(INFO_LOG, "node:%s; children are present; node cannot be deleted.delete Failed", m_strZooDistributedRootPath.c_str());
                    }
                }
            }
        }
    }
}

// Init Zookeeper
bool CZooDistributedLock::initZookeeper(const char* host, int recv_timeout, int flags)
{
    CLogManager::getSingleton().AddLog(INFO_LOG, "%s", __FUNCTION__);
    m_pZooHandle = zookeeper_init(host,initZooWatch,recv_timeout,nullptr,nullptr,flags);
    if(!m_pZooHandle)
        return false;

    zoo_set_debug_level(ZOO_LOG_LEVEL_DEBUG);
    return true;
}

// init callback
void CZooDistributedLock::initZooWatch(zhandle_t* zh, int type, int state, const char* path, void* watcherCtx)
{
    CLogManager::getSingleton().AddLog(INFO_LOG, "%s", __FUNCTION__);
    /* Be careful using zh here rather than zzh - as this may be mt code
    * the client lib may call the watcher before zookeeper_init returns */
    std::cout << "Watcher " << type2String(type) <<" state = " << state2String(state);
    CLogManager::getSingleton().AddLog(INFO_LOG, "%s:Watch:%s;state:%s", __FUNCTION__,type2String(type), state2String(state));
    if(path && strlen(path) > 0)
    {
        std::cout << " for path:" << path << "\n";
        CLogManager::getSingleton().AddLog(INFO_LOG, "   for path:%s", path);
    }
    std::cout << std::endl;

    m_bZooHandleClosed = true;
    if(type == ZOO_SESSION_EVENT)
    {
        if(state == ZOO_CONNECTED_STATE)
        {
            //const clientid_t* id = zoo_client_id(zh);
            m_bZooHandleClosed = false;
        }
        else if(state == ZOO_AUTH_FAILED_STATE)
        {
            std::cout << "Authentication failure. Shutting down...\n";
            CLogManager::getSingleton().AddLog(ERROR_LOG, "%s:Authentication failure. Shutting down...", __FUNCTION__);
            zookeeper_close(zh);m_bZooHandleClosed = true;
            zh = nullptr;m_pZooHandle = nullptr;
        }
        else if(state == ZOO_EXPIRED_SESSION_STATE)
        {
            std::cout << "Session expired. Shutting down...\n";
            CLogManager::getSingleton().AddLog(ERROR_LOG, "%s:Session expired. Shutting down...", __FUNCTION__);
            zookeeper_close(zh);m_bZooHandleClosed = true;
            zh = nullptr;m_pZooHandle = nullptr;
        }
    }
}

// Create lock root
void CZooDistributedLock::createZooDistributedRoot(const char* rootpath)
{
    CLogManager::getSingleton().AddLog(INFO_LOG, "%s", __FUNCTION__);
    //create persistent node
    m_strZooDistributedRootPath = rootpath;
    int ret = zoo_acreate(m_pZooHandle,rootpath,rootpath,strlen(rootpath),
        &ZOO_OPEN_ACL_UNSAFE,0,createNodeWatch,nullptr);
    if(ret)
    {
        std::cout << __FUNCTION__ << ":Error:" << ret << std::endl;
        CLogManager::getSingleton().AddLog(ERROR_LOG, "%s:Error:%d", __FUNCTION__, ret);
        return;
    }
}

// 0 false;1 success; 2 alreadyExisting
void CZooDistributedLock::createNodeWatch(int rc, const char* name, const void* data)
{
    CLogManager::getSingleton().AddLog(INFO_LOG, "%s:%s", __FUNCTION__,rc2String(rc));
    if(rc == ZOK)
    {
        m_nZooDistributedRootCreated = 1;
        std::cout << "name:" << name << " distributed root node create success" << std::endl;
        CLogManager::getSingleton().AddLog(INFO_LOG, "%s:name:%s distributed root node create success", __FUNCTION__, name);
        std::cout << "Create Lock Sub temp Sequence Node\n";
        CLogManager::getSingleton().AddLog(INFO_LOG, "%s:Create Lock Sub temp Sequence Node", __FUNCTION__);
        createZooLockSubNode();
    }
    else if(rc == ZNODEEXISTS)
    {
        m_nZooDistributedRootCreated = 2;
        std::cout << "distributed root node already exists" << std::endl;
        CLogManager::getSingleton().AddLog(INFO_LOG, "%s:distributed root node already exists", __FUNCTION__);
        std::cout << "Create Lock Sub temp Sequence Node\n";
        CLogManager::getSingleton().AddLog(INFO_LOG, "%s:Create Lock Sub temp Sequence Node", __FUNCTION__);
        createZooLockSubNode();
    }
    else
    {
        m_nZooDistributedRootCreated = 0;
        CLogManager::getSingleton().AddLog(ERROR_LOG, "%s:distributed root node create Error", __FUNCTION__);
    }
}

// Create lock root
void CZooDistributedLock::createZooLockSubNode()
{
    CLogManager::getSingleton().AddLog(INFO_LOG, "%s", __FUNCTION__);
    //create ZOO_SEQUENCE node
    std::string tempNode = m_strZooDistributedRootPath;
    if(tempNode.at(tempNode.size() - 1) != '/')
        tempNode.append("/lock-");
    int ret = zoo_acreate(m_pZooHandle,tempNode.c_str(),nullptr,0,
        &ZOO_OPEN_ACL_UNSAFE,ZOO_EPHEMERAL|ZOO_SEQUENCE,createSubNodeWatch,nullptr);
    if(ret)
    {
        std::cout << __FUNCTION__ << ":Error:" << ret << std::endl;
        CLogManager::getSingleton().AddLog(ERROR_LOG, "%s:Error:%d", __FUNCTION__, ret);
        return;
    }
}

// 0 false;1 success; 2 alreadyExisting
void CZooDistributedLock::createSubNodeWatch(int rc, const char* name, const void* data)
{
    CLogManager::getSingleton().AddLog(INFO_LOG, "%s:%s", __FUNCTION__,rc2String(rc));
    if(rc == ZOK)
    {
        std::cout << "name:" << name << " distributed sub ZOO_SEQUENCE node create success" << std::endl;
        CLogManager::getSingleton().AddLog(INFO_LOG, "%s:name:%s distributed sub ZOO_SEQUENCE node create success", __FUNCTION__,name);
        m_strSequenceNodeName = name;
        int npos = m_strSequenceNodeName.find_last_of("lock-");
        if (npos != std::string::npos)
        {
            std::string nodeNum = m_strSequenceNodeName.substr(npos + 1, m_strSequenceNodeName.size() - npos - 1);
            m_nSequenceNodeNum = atoi(nodeNum.c_str());

            if(m_nSequenceNodeNum == 0)
            {
                m_nDistributedState = 2;
            }
            else
            {
                m_nDistributedState = 1;
                int preNode = m_nSequenceNodeNum - 1;
                char cPath[128];
                sprintf_s(cPath,127,"%s%010d", m_strSequenceNodeName.substr(0, npos+1).c_str(), preNode);
                CLogManager::getSingleton().AddLog(INFO_LOG, "%s:Monitor:%s", __FUNCTION__,cPath);
                zoo_awexists(m_pZooHandle, cPath, subNodeGetWatch, nullptr, dataCompletion, nullptr);
            }
        }
    }
    // no enter
    else if(rc == ZNODEEXISTS)
    {
        std::cout << "distributed sub ZOO_SEQUENCE node already exists" << std::endl;
        CLogManager::getSingleton().AddLog(ERROR_LOG, "%s:distributed sub ZOO_SEQUENCE node already exists", __FUNCTION__);
    }
}

void CZooDistributedLock::subNodeGetWatch(zhandle_t* zh, int type, int state, const char* path, void* watcherCtx)
{
    CLogManager::getSingleton().AddLog(INFO_LOG, "%s", __FUNCTION__);
    std::cout << "type:" << type << "; state:" << stat << "; path:" << path << std::endl;
    std::cout << "Watcher " << type2String(type) << " state = " << state2String(state);
    CLogManager::getSingleton().AddLog(INFO_LOG, "%s:Watch:%s;state:%s", __FUNCTION__,type2String(type), state2String(state));
    if(type == ZOO_CHANGED_EVENT)
    {
        std::cout << "path:" << path << "; Changed" << std::endl;
        CLogManager::getSingleton().AddLog(INFO_LOG, "path:%s; Changed", path);
    }       
    else if(type == ZOO_DELETED_EVENT)
    {
        std::cout << "path:" << path << "; Deleted" << std::endl;
        CLogManager::getSingleton().AddLog(INFO_LOG, "path:%s; Deleted", path);
        m_nDistributedState = 2;
        std::cout << "Node:" << m_strSequenceNodeName << " becomes master" << std::endl;
        CLogManager::getSingleton().AddLog(INFO_LOG, "Node:%s becomes master", m_strSequenceNodeName.c_str());
    }
    else if (type == ZOO_CREATED_EVENT)
    {
        std::cout << "path:" << path << "; Created" << std::endl;
        CLogManager::getSingleton().AddLog(INFO_LOG, "path:%s; Created", path);
    }
    else if (type == ZOO_CHILD_EVENT)
    {
        std::cout << "path:" << path << "; Child Event" << std::endl;
        CLogManager::getSingleton().AddLog(INFO_LOG, "path:%s; Child Event", path);
    }
    else if (type == ZOO_SESSION_EVENT)
    {
        std::cout << "path:" << path << "; Session Event" << std::endl;
        CLogManager::getSingleton().AddLog(INFO_LOG, "path:%s; Session Event", path);
    }
    else if (type == ZOO_NOTWATCHING_EVENT)
    {
        std::cout << "path:" << path << "; NotWatching Event" << std::endl;
        CLogManager::getSingleton().AddLog(INFO_LOG, "path:%s; NotWatching Event", path);
    }
}

void CZooDistributedLock::dataCompletion(int rc,const struct Stat* stat, const void* data)
{
    CLogManager::getSingleton().AddLog(INFO_LOG, "%s:%s", __FUNCTION__,rc2String(rc));
    //std::cout << "rc:" << rc << ";stat(ctime):" << stat->ctime << std::endl;
    std::cout << "rc:" << rc << std::endl;
    CLogManager::getSingleton().AddLog(INFO_LOG, "%s:rc=%d", __FUNCTION__, rc);
    if(rc == ZNONODE)
    {
        m_nDistributedState = 2;
        std::cout << "Node:" << m_strSequenceNodeName << " becomes master" << std::endl;
        CLogManager::getSingleton().AddLog(INFO_LOG, "%s:Node:%s becomes master", __FUNCTION__,m_strSequenceNodeName.c_str());
    }
}

void CZooDistributedLock::deleteNodecompletion(int rc, const void* data)
{
    CLogManager::getSingleton().AddLog(INFO_LOG, "%s:%s", __FUNCTION__,rc2String(rc));
    if (rc == ZOK)
    {
        std::cout << "node:" << m_strZooDistributedRootPath << " delete success\n";
        CLogManager::getSingleton().AddLog(INFO_LOG, "node:%s; delete success", m_strZooDistributedRootPath.c_str());
        m_nZooDistributedRootCreated = 0;
        m_nSequenceNodeNum = -1;
        m_nDistributedState = 0;
    }
    else if (rc == ZNONODE)
    {
        std::cout << "node:" << m_strZooDistributedRootPath << " not exists.delete Failed\n";
        CLogManager::getSingleton().AddLog(INFO_LOG, "node:%s; not exists.delete Failed", m_strZooDistributedRootPath.c_str());
    }
    else if (rc == ZNOAUTH)
    {
        std::cout << "node:" << m_strZooDistributedRootPath << ". the client does not have permission.delete Failed\n";
        CLogManager::getSingleton().AddLog(INFO_LOG, "node:%s; the client does not have permission.delete Failed", m_strZooDistributedRootPath.c_str());
    }
    else if (rc == ZBADVERSION)
    {
        std::cout << "node:" << m_strZooDistributedRootPath << " expected version does not match actual version.delete Failed\n";
        CLogManager::getSingleton().AddLog(INFO_LOG, "node:%s; expected version does not match actual version.delete Failed", m_strZooDistributedRootPath.c_str());
    }
    else if (rc == ZNOTEMPTY)
    {
        std::cout << "node:" << m_strZooDistributedRootPath << " children are present; node cannot be deleted.delete Failed\n";
        CLogManager::getSingleton().AddLog(INFO_LOG, "node:%s; children are present; node cannot be deleted.delete Failed", m_strZooDistributedRootPath.c_str());
    }
}

void CZooDistributedLock::getChildrenCompletion(int rc,const struct String_vector* strings,const void* data)
{
    CLogManager::getSingleton().AddLog(INFO_LOG, "%s:%s", __FUNCTION__,rc2String(rc));
    if(rc == ZOK)
    {
        if(strings)
        {
            // Node children
            std::cout << "Node Children:" << m_strZooDistributedRootPath << "\n";
            CLogManager::getSingleton().AddLog(INFO_LOG, "%s:Node Children:%s", __FUNCTION__,m_strZooDistributedRootPath.c_str());
            for(int i = 0; i < strings->count; i++)
            {
                std::cout << "\t" << strings->data[i] << "\n";
                CLogManager::getSingleton().AddLog(INFO_LOG, "\t%s", strings->data[i]);
            }

            if(strings->count == 1)
                zoo_adelete(m_pZooHandle, m_strZooDistributedRootPath.c_str(), -1, deleteNodecompletion, nullptr);
        }
    }
}

// Get Version
const char* CZooDistributedLock::getZooKeeperVersion()
{
    CLogManager::getSingleton().AddLog(INFO_LOG, "%s", __FUNCTION__);
    static char cVersion[128];
    memset(cVersion, 0, 128);
    sprintf_s(cVersion,
        "Version: ZooKeeper version %d.%d.%d\n",
        ZOO_MAJOR_VERSION,
        ZOO_MINOR_VERSION,
        ZOO_PATCH_VERSION);

    return cVersion;
}

const char* CZooDistributedLock::state2String(int state)
{
    if(state == 0)
        return "CLOSED_STATE";
    if(state == ZOO_CONNECTING_STATE)
        return "CONNECTING_STATE";
    if(state == ZOO_ASSOCIATING_STATE)
        return "ASSOCIATING_STATE";
    if(state == ZOO_CONNECTED_STATE)
        return "CONNECTED_STATE";
    if(state == ZOO_EXPIRED_SESSION_STATE)
        return "EXPIRED_SESSION_STATE";
    if(state == ZOO_AUTH_FAILED_STATE)
        return "AUTH_FAILED_STATE";

    return "INVALID_STATE";
}

const char* CZooDistributedLock::type2String(int state)
{
    if(state == ZOO_CREATED_EVENT)
        return "CREATED_EVENT";
    if(state == ZOO_DELETED_EVENT)
        return "DELETED_EVENT";
    if(state == ZOO_CHANGED_EVENT)
        return "CHANGED_EVENT";
    if(state == ZOO_CHILD_EVENT)
        return "CHILD_EVENT";
    if(state == ZOO_SESSION_EVENT)
        return "SESSION_EVENT";
    if(state == ZOO_NOTWATCHING_EVENT)
        return "NOTWATCHING_EVENT";

    return "UNKNOWN_EVENT_TYPE";
}

const char* CZooDistributedLock::rc2String(int rc)
{
    if (rc == ZOK)
        return "ZOK";
    if (rc == ZSYSTEMERROR)
        return "ZSYSTEMERROR";
    if (rc == ZCONNECTIONLOSS)
        return "ZCONNECTIONLOSS";
    if (rc == ZOPERATIONTIMEOUT)
        return "ZOPERATIONTIMEOUT";
    if (rc == ZINVALIDSTATE)
        return "ZINVALIDSTATE";
    if (rc == ZNONODE)
        return "ZNONODE";
    if (rc == ZNOAUTH)
        return "ZNOAUTH";
    if (rc == ZNODEEXISTS)
        return "ZNODEEXISTS";
    if (rc == ZNOTEMPTY)
        return "ZNOTEMPTY";
    if (rc == ZSESSIONEXPIRED)
        return "ZSESSIONEXPIRED";

    return "UNKNOWN_RC";
}