#include "TimerWatchdog.h"

#include "swss/logger.h"

#include <chrono>

using namespace syncd;

TimerWatchdog::TimerWatchdog(
        _In_ int64_t warnTimespan):
    m_run(true),
    m_warnTimespan(warnTimespan),
    m_callback(nullptr)
{
    SWSS_LOG_ENTER();

    int64_t start = getTimeSinceEpoch();
    m_startTimestamp = start;
    m_endTimestamp = start;
    m_lastCheckTimestamp = start;

    m_thread = std::make_shared<std::thread>(&TimerWatchdog::threadFunction, this);

    // m_thread->detach()
}

TimerWatchdog::~TimerWatchdog()
{
    SWSS_LOG_ENTER();

    m_run = false;

    m_thread->join();
}

void TimerWatchdog::setStartTime()
{
    SWSS_LOG_ENTER();

    do 
    {
        m_startTimestamp = getTimeSinceEpoch();
    } 
    while (m_startTimestamp <= m_endTimestamp); // make sure new start time is always past last end time

}

void TimerWatchdog::setEndTime()
{
    SWSS_LOG_ENTER();

    do
    {
        m_endTimestamp = getTimeSinceEpoch();
    }
    while (m_endTimestamp <= m_startTimestamp); // make sure new end time is always past last start time
}

void TimerWatchdog::setCallback(
        _In_ std::function<void(uint64_t)> callback)
{
    SWSS_LOG_ENTER();

    m_callback = callback;
}

void TimerWatchdog::threadFunction()
{
    SWSS_LOG_ENTER();

    SWSS_LOG_NOTICE("starting timer watchdog thread");

    while (m_run)
    {
        std::this_thread::sleep_for(std::chrono::seconds(1));

        // we make local copies, since executing functions can be so fast that
        // when we will read second time start timestamp it can be different
        // than previous one
 
        int64_t start = m_startTimestamp;
        int64_t end = m_endTimestamp;
        int64_t now = getTimeSinceEpoch(); // now needs to be obtained after obtaining start

        int64_t span = end - start;

        if (span < 0 && start > m_lastCheckTimestamp)
        {
            // this means start > end, so new function is currently executing,
            // or that function hanged, so see how long that function is
            // executing, this negative span can be arbitrary long even hours,
            // and that is fine, since we don't know when OA makes next
            // function call

            span = now - start; // this must be always non negative

            SWSS_LOG_NOTICE("time span %ld ms", span/1000);

            if (span < 0)
                SWSS_LOG_THROW("negative span 'now - start': %ld - %ld", now, start);

            if (span > m_warnTimespan)
            {
                m_lastCheckTimestamp = start;

                // function probably hanged

                SWSS_LOG_WARN("timespan WD exceeded %ld ms", span/1000);

                auto callback = m_callback;

                if (callback)
                    callback(span);
            }

            continue;
        }

        m_lastCheckTimestamp = start;
    }

    SWSS_LOG_NOTICE("ending timer watchdog thread");
}

int64_t TimerWatchdog::getTimeSinceEpoch()
{
    SWSS_LOG_ENTER();

    return std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now().time_since_epoch()).count();
}
