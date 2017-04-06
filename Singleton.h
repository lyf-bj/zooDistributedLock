#ifndef _SINGLETON_H_
#define _SINGLETON_H_

#include <assert.h>

template <typename T> class Singleton
{
public:
    Singleton(void)
    {
        assert(!msSingleton);
#if defined( _MSC_VER ) && _MSC_VER < 1200   
        int offset = (int)(T*)1 - (int)(Singleton <T>*)(T*)1;
        msSingleton = (T*)((int)this + offset);
#else
        msSingleton = static_cast< T* >( this );
#endif
    }
    ~Singleton(void)
    {  assert(msSingleton); msSingleton = 0;  }
    static T& getSingleton(void)
    {   assert(msSingleton);  return (*msSingleton); }
    static T* getSingletonPtr(void)
    { return msSingleton; }

private:
    /** @brief Explicit private copy constructor. This is a forbidden operation.*/
    Singleton(const Singleton<T> &);

    /** @brief Private operator= . This is a forbidden operation. */
    Singleton& operator=(const Singleton<T> &);

protected:
    static T* msSingleton;
};

#endif