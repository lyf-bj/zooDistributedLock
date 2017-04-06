#ifndef _LOGMANAGER_H
#define _LOGMANAGER_H

#include <string>
#include "log4cplus/logger.h"
#include "log4cplus/fileappender.h"
#include "log4cplus/consoleappender.h"
#include "log4cplus/layout.h"
#include "log4cplus/loggingmacros.h"
using namespace log4cplus;
using namespace log4cplus::helpers;

#include "Singleton.h"

enum LOGLEVEL
{
    OFF_LOG     = 60000,
    FATAL_LOG   = 50000,
    ERROR_LOG   = 40000,
    WARN_LOG    = 30000,
    INFO_LOG    = 20000,
    DEBUG_LOG   = 10000,
    TRACE_LOG   = 0,
    ALL_LOG     = 0,
    NOT_SET_LOG = -1
};

class CLogManager : public Singleton<CLogManager>
{
public:
	CLogManager();
	virtual ~CLogManager();

	static CLogManager& getSingleton(void);
    static CLogManager* getSingletonPtr(void);

    static void GetCurrentPath(std::string& strPath, std::string& strName);


    void AddLog(int iLevel,std::string strlog);
    void AddLog(int iLevel,const char* msg, ...);

private:
    CLogManager(const CLogManager& object);
    CLogManager& operator =(const CLogManager& object);

    void InitLog();

private:
    Logger m_pAppLogger;
};

#endif
