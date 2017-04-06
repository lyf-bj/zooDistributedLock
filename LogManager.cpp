#include "LogManager.h"
#include <memory.h>
#ifdef WIN32
    #include <Windows.h>
    #include <tlhelp32.h>
    #include <io.h>
    #include <direct.h>
    #define WIN32_LEAN_AND_MEAN
    #define DIR_SLASH '\\'
#else
    #define DIR_SLASH '/'
#endif

template<> CLogManager* Singleton<CLogManager>::msSingleton = 0;

CLogManager* CLogManager::getSingletonPtr(void)
{
	return msSingleton;
}

CLogManager& CLogManager::getSingleton(void)
{  
    assert( msSingleton  );
    return ( *msSingleton  );
}

CLogManager::CLogManager()
{
    InitLog();
}

CLogManager::~CLogManager()
{
	m_pAppLogger.shutdown();
}

void CLogManager::InitLog()
{
    std::string strmodule, strPath;
    GetCurrentPath(strPath, strmodule);

    char al[50];memset(al,0,50);
    char cc[20];memset(cc,0,20);
    const time_t t = time(NULL);
    struct tm current_time;
    localtime_s(&current_time, &t);
    sprintf_s(cc,20,"%04d%02d%02d_%02d%02d%02d",current_time.tm_year + 1900,current_time.tm_mon + 1,current_time.tm_mday,\
        current_time.tm_hour,current_time.tm_min,current_time.tm_sec);
    sprintf_s(al,50,"%s_%s.log", strmodule.c_str(),cc);
    SharedAppenderPtr pFileAppender(new FileAppender(al));

    //std::auto_ptr<Layout>  _Datalayout(new SimpleLayout());
	std::string  pattern = "%D{%Y%m%d %H:%M:%S} - %m%n";
    log4cplus::Layout* layout_2 = new PatternLayout(pattern);
    pFileAppender->setLayout(std::unique_ptr<log4cplus::Layout>(layout_2));

    // Logger   
    m_pAppLogger = Logger::getInstance("logManger");
    int m_iLogLevel = ALL_LOG;
    m_pAppLogger.setLogLevel(m_iLogLevel);

    // Appender
    m_pAppLogger.addAppender(pFileAppender);
}

void CLogManager::AddLog(int iLevel,std::string strlog)
{
    m_pAppLogger.log(iLevel,strlog.c_str());
}

void CLogManager::AddLog(int iLevel,const char* msg, ...)
{
    char buf[1024];
    va_list argp;
    va_start(argp, msg );
    vsprintf_s(buf,1024,msg,argp);
    va_end( argp );

    m_pAppLogger.log(iLevel,buf);
}

void CLogManager::GetCurrentPath(std::string& strPath, std::string& strName)
{
#if WIN32
    char exeFullPath[MAX_PATH];

    ::GetModuleFileName(NULL, exeFullPath, MAX_PATH);
    std::string strFullPath(exeFullPath);
    int pos = strFullPath.find_last_of(DIR_SLASH, strFullPath.length());

    strPath = strFullPath.substr(0, pos + 1);
    strName = strFullPath.substr(pos + 1, strFullPath.length() - pos - 5);

#else
    const int MAXBUFSIZE = 1024;
    char buf[MAXBUFSIZE];
    int count = readlink("/proc/self/exe", buf, MAXBUFSIZE);
    if (count < 0 || count >= MAXBUFSIZE)
    {
        std::cout << "Failed\n";
        return;
    }
    buf[count] = '\0';

    std::string strFullPath = buf;
    int pos = strFullPath.find_last_of(DIR_SLASH, strFullPath.length());

    strPath = strFullPath.substr(0, pos + 1);
    strName = strFullPath.substr(pos + 1, strFullPath.length() - pos - 1);
#endif
}