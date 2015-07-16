#include "sample.h"

#include <iostream>
#include <sys/time.h>
#include <ctime>
#include <string>
#include <cstring>
#include <fstream>
#include <iomanip>
#include <unistd.h>
#include <list>
#include <pthread.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <stdio.h>
#include <errno.h>
#include <syslog.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <cstdlib>

#include <libecap/common/registry.h>
#include <libecap/common/errors.h>
#include <libecap/common/header.h>
#include <libecap/common/message.h>
#include <libecap/common/body.h>
#include <libecap/common/body_size.h>
#include <libecap/common/names.h>
#include <libecap/common/named_values.h>
#include <libecap/adapter/service.h>
#include <libecap/adapter/xaction.h>
#include <libecap/host/xaction.h>

#define LIBECAP_VERSION_020

namespace compress
{

    struct ZLogRec
	{
        double time_point;
        long process_time;
        std::string address;
        std::string flags;
        long orig_size;
        long comp_size;
        std::string method;
        std::string url;

		ZLogRec() : time_point(0), process_time(0), orig_size(0), comp_size(0) {}
	};


	struct StatRec
	{
        double time_point;
        std::string address;
		long orig_size;
        long comp_size;
        std::string method;
        std::string url;
        std::string host_addr;
		int fail_count;

		StatRec() : time_point(0), orig_size(0), comp_size(0), fail_count(0) {}
	};


    enum LogPriority {
        logPrioEmerg   = LOG_EMERG,
        logPrioAlert   = LOG_ALERT,
        logPrioCrit    = LOG_CRIT,
        logPrioErr     = LOG_ERR,
        logPrioWarn    = LOG_WARNING,
        logPrioNotice  = LOG_NOTICE,
        logPrioInfo    = LOG_INFO,
        logPrioDebug   = LOG_DEBUG,
    };


    // stream buffer for logging to syslog

    class LogStream : public std::streambuf
    {
    public:
        enum {logBufferSize = 4096,};

        explicit LogStream(const std::string &subModuleName,
                           int defaultPriority = LOG_INFO,
                           // the syslog stuff
                           const std::string &ident = "", int logopt = 0, int facility = 0,
                           // should be syslog closed while object delete or not
                           bool closeLogOnDelete = false);
        explicit LogStream(const LogStream &rhs);

        inline const char *subModuleIdent() const;

        ~LogStream();

    protected:
        int sync();
        int overflow(int ch);

    private:
        char mBuffer[logBufferSize];
        int mPriority;
        int mFacility;
        bool mCloseLogOnDelete;
        char mIdent[logBufferSize];
        std::string mIdentSubModule;

        friend std::ostream& operator<< (std::ostream& os, const LogPriority& logPriority);
    };

    std::ostream& operator<<(std::ostream& os, const LogPriority& logPriority)
    {
        static_cast<LogStream*>(os.rdbuf())->mPriority = static_cast<int>(logPriority);
        return os;
    }


	template<typename T>
	class AliasT
	{
	public:
        AliasT(const T &value);
        std::ostream &print(std::ostream &os) const;

    private:
        T val;
    };

    template<typename T>
    std::ostream& operator<<(std::ostream& os, const AliasT<T>& val)
    {
        val.print(os);
        return os;
    }

    template<>
    std::ostream &compress::AliasT<double>::print(std::ostream &os) const;

    template<>
    std::ostream &compress::AliasT<std::size_t>::print(std::ostream &os) const;

    template<>
    std::ostream &compress::AliasT<std::string>::print(std::ostream &os) const;

    template<typename T>
    AliasT<T> fmt(const T &val)
    {
        compress::AliasT<T> aliased(val);
        return aliased;
    }

	class Logger
	{
	public:
        static std:: string strTime();
		static double timePoint();

        Logger(const std::string &logtag = default_logtag, int log_prio = default_log_prio);
        // Logger();
        // Logger(const std::string &log_file);
		~Logger();

        inline std::ostream &debug();
        inline std::ostream &info();
        inline std::ostream &warn();
        inline std::ostream &err();
        inline std::ostream &crit();

        inline void ufmt();
    private:
        static const std::string default_logtag;
        static const int default_log_prio = LOG_INFO;
        LogStream stream_buf;
        std::ostream log_stream;
        LogPriority max_log_prio;
        // static const std::string default_logname;
        // const std::string log_name;
        // std::ofstream log_stream;

		inline std::ostream &ols();
		inline bool open();
	};


	// ziproxy output handler
	class ZLogData
	{
	public:
		ZLogData(const char* fname, double lower_bound = 0);
        ~ZLogData();

        inline bool read();

        bool find(const StatRec &stat, double range = 1.0);
		StatRec *find(std::list<StatRec*> &stats, double range = 1.0);

        inline const ZLogRec &getRec() const;

	private:
        std::ifstream zlog;
        ZLogRec rec;
		double watermark;
	};


    struct StatBurst
    {
        const std::string name;
        const long send_time;

        StatBurst(const std::string &fname, long time_point) : name(fname), send_time(time_point) {}
        StatBurst(const char *fname, long time_point) : name(fname), send_time(time_point) {}
    };


    class Storage
    {
    public:
        Storage(long step, const std::string &file_prefix = "/tmp/compstatburst");
        ~Storage();

        bool add(const StatRec &stat);

        void flush(bool close = false);

        long step() const;
        void step(long new_val);

    private:
        enum{postfix_size = 20,};

        static const char *agent_addr;

        char *storage_fname, *fname_postfix;
        std::ofstream storage_stream;
        long storage_threshold;
        long threshold_step;

        int sock;
        struct sockaddr_un server;

        std::list<StatBurst> fq;

        bool init(long start, long step);
        bool send(const std::string &fname);
    };


	// adapter service
	class Service: public libecap::adapter::Service
	{
	public:
		Service();
		virtual ~Service();

		// About
		virtual std::string uri() const; // unique across all vendors
		virtual std::string tag() const; // changes with version and config
		virtual void describe(std::ostream &os) const; // free-format info

		// Configuration
		virtual void configure(const libecap::Options &cfg);
        virtual void reconfigure(const libecap::Options &cfg);
        void setOne(const libecap::Name &name, const libecap::Area &valArea);

		// Lifecycle
		virtual void start(); // expect makeXaction() calls
		virtual void stop(); // no more makeXaction() calls until start()
		virtual void retire(); // no more makeXaction() calls

		// Scope (XXX: this may be changed to look at the whole header)
		virtual bool wantsUrl(const char *url) const;

        // Work
#ifdef LIBECAP_VERSION_020
        virtual libecap::adapter::Xaction *makeXaction(libecap::host::Xaction *hostx);
#else
        virtual MadeXactionPointer makeXaction(libecap::host::Xaction *hostx);
#endif

		void addStat(StatRec *stat);

	private:
        static const char *zlog_fname;
		static const char *agent_addr;
        static const int url_alive_time = 5; // seconds
        static const long default_burst_step = 5 * 60; // seconds

        // static const double tp_range;
        static const int stats_max_count;
		static const int stat_max_failure;
        //std::map<long,
        std::list<StatRec*> stats;
		pthread_mutex_t stats_lock;

        Storage storage;

        bool setDataBurstDelay(const std::string &value);

		void handleStats(bool final = false);
		void handleStatsBack(bool final = false);
    };


    // Calls Service::setOne() for each host-provided configuration option.
    // See Service::configure().
    class Cfgtor: public libecap::NamedValueVisitor {
    public:
        Cfgtor(Service &aSvc): svc(aSvc) {}
        virtual void visit(const libecap::Name &name, const libecap::Area &value) {
            svc.setOne(name, value);
        }
        Service &svc;
    };


// adapter transaction
	class Transact: public libecap::adapter::Xaction
	{
	public:
		Transact(libecap::shared_ptr<Service> aService, libecap::host::Xaction *x);
		virtual ~Transact();

		// meta-info for the host transaction
		virtual const libecap::Area option(const libecap::Name &name) const;
		virtual void visitEachOption(libecap::NamedValueVisitor &visitor) const;

		// lifecycle
		virtual void start();
		virtual void stop();

		// adapted body transmission control
		virtual void abDiscard() {}
		virtual void abMake() {}
		virtual void abMakeMore() {}
		virtual void abStopMaking() {}

		// adapted body content extraction and consumption
		virtual libecap::Area abContent(libecap::size_type, libecap::size_type) { return libecap::Area(); }
		virtual void abContentShift(libecap::size_type)  {}

		// virgin body state notification
		virtual void noteVbContentDone(bool) {}
        virtual void noteVbContentAvailable() {}

#ifdef LIBECAP_VERSION_020
        // libecap::Callable API, via libecap::host::Xaction
        virtual bool callable() const;
#endif

	private:
		libecap::shared_ptr<Service> service;
		libecap::host::Xaction *hostx; // Host transaction rep
		bool reqmode;
		std::size_t body_size;

		// double timePoint() const;
		bool fillData();
	};


} // namespace stat



// static data

const std::string compress::Logger::default_logtag = "compress_adapter";
const char *compress::Storage::agent_addr = "/tmp/statistic_adapter.ipc";
const int compress::Service::stats_max_count = 10;
const int compress::Service::stat_max_failure = 20;
const char *compress::Service::zlog_fname = "/var/log/ziproxy/access.log";



// stream buffer for logging to syslog

compress::LogStream::LogStream(const std::string &subModuleName, int defaultPriority,
                           const std::string &ident, int logopt, int facility,
                           bool closeLogOnDelete)
    : mPriority(defaultPriority), mFacility(facility),
      mCloseLogOnDelete(closeLogOnDelete), mIdentSubModule(subModuleName)
{
    char *base = mBuffer;
    setp(base, base + logBufferSize - 1); // reserve 1 byte for \0
    // syslog stuff
    if(ident.empty()) {
        return;
    }
    std::strncpy(mIdent, ident.c_str(), logBufferSize - 1);
    mIdent[logBufferSize - 1] = '\0';
    openlog(mIdent, logopt, mFacility);
}


compress::LogStream::LogStream(const LogStream &rhs)
    : mPriority(rhs.mPriority), mFacility(rhs.mFacility),
      mCloseLogOnDelete(false), mIdentSubModule(rhs.mIdentSubModule)
{
    char *base = mBuffer;
    setp(base, base + logBufferSize - 1); // reserve 1 byte for \0
    mIdent[0] = '\0';
}


compress::LogStream::~LogStream()
{
    if(mCloseLogOnDelete)
        closelog();
}


int compress::LogStream::sync()
{
    if(pbase() != pptr())
    {
        *pptr() = '\0';
        syslog(mPriority, "%s: %s", mIdentSubModule.c_str(), pbase());
        std::ptrdiff_t delta = pptr() - pbase();
        pbump(-delta);
    }
    return 0;
}


int compress::LogStream::overflow(int ch)
{
    if(ch != traits_type::eof()) {
        *pptr() = ch;
        pbump(1);
    }
    sync();
    return ch;
}


inline const char *compress::LogStream::subModuleIdent() const
{
    return mIdentSubModule.c_str();
}


// logger and type alias

template<typename T>
compress::AliasT<T>::AliasT(const T &value)
    : val(value)
{
}


template<typename T>
std::ostream &compress::AliasT<T>::print(std::ostream &os) const
{
    os << val;
    return os;
}


template<>
std::ostream &compress::AliasT<double>::print(std::ostream &os) const
{
    try{
        os << std::fixed << std::setw(14) << std::setprecision(3) << std::setfill('0') << val;
    }
    catch(...){}
    return os;
}


template<>
std::ostream &compress::AliasT<std::size_t>::print(std::ostream &os) const
{
    try{
        os << std::setw(3) << std::setfill('0') << val;
    }
    catch(...) {}
    return os;
}

template<>
std::ostream &compress::AliasT<std::string>::print(std::ostream &os) const
{
    try {
        static const std::size_t max_str_len = 35;
        os << val.substr(0, max_str_len);
        if(val.size() > max_str_len) {
            os << "...";
        }
    } catch(...) {}
    return os;
}


// logger

std:: string compress::Logger::strTime()
{
	time_t rawtime;
	struct tm * timeinfo;
	char buffer[80];

	time (&rawtime);
	timeinfo = localtime(&rawtime);

	strftime(buffer,80,"%d.%m.%Y %I:%M:%S",timeinfo);
	return buffer;
}


double compress::Logger::timePoint()
{
	struct timeval t;
	gettimeofday(&t, NULL);
	// long long mt = t.tv_sec * 1000 * 1000 + t.tv_usec;
	double mt = t.tv_sec + (t.tv_usec / 1000000.000);
	return mt;
}


compress::Logger::Logger(const std::string &logtag, int log_prio)
    : stream_buf(logtag), log_stream(&stream_buf)
{
}


// compress::Logger::Logger()
//    : log_name(default_logname), log_stream(default_logname.c_str(), std::ofstream::app)
// {
// }


// compress::Logger::Logger(const std::string &log_file)
// 	: log_name(log_file), log_stream(log_file.c_str(), std::ofstream::app)
// {
// }


compress::Logger::~Logger()
{
    // if(log_stream.is_open()){
    // 	log_stream.close();
    // }
}


inline std::ostream &compress::Logger::debug()
{
    return ols() << logPrioDebug << "[DEBUG] ";
}


inline std::ostream &compress::Logger::info()
{
    // return ols() << fmt(timePoint()) << " " << logPrioInfo;
    return ols() << logPrioInfo;
}


inline std::ostream &compress::Logger::warn()
{
    return ols() << logPrioWarn;
}


inline std::ostream &compress::Logger::err()
{
    return ols() << logPrioErr;
}


inline std::ostream &compress::Logger::crit()
{
    return ols() << logPrioCrit;
}


// template<typename T>
// compress::AliasT<T> compress::Logger::fmt(const T &val) const
// {
//     compress::AliasT<T> aliased(val);
//     return aliased;
// }


inline void compress::Logger::ufmt()
{
	ols() << std::setw(-1);
	ols().unsetf(std::ios_base::floatfield);
}


inline std::ostream &compress::Logger::ols()
{
	if(log_stream.bad()) {
		return std::cout;
	} else {
		return log_stream;
	}
}


inline bool compress::Logger::open()
{
    // if(!log_stream.is_open()){
    // 	log_stream.open(log_name.c_str(), std::ofstream::app);
    // }
	return log_stream.good();
}



// ziproxy output handler

compress::ZLogData::ZLogData(const char* fname, double lower_bound )
	: zlog(fname, std::ios_base::in), watermark(lower_bound)
{
    // Logger log;
    try {
        while(read()) {
            if(watermark <= rec.time_point) {
                break;
            }
        }
    }
    catch(...){
        // log.err() << "Got exception while reach watermark in ziproxy output" << std::endl;
    }
}


compress::ZLogData::~ZLogData()
{
	if(zlog.is_open()){
		zlog.close();
	}
}


inline bool compress::ZLogData::read()
{
    if(!zlog.good()) {
        return false;
    }

	zlog >> rec.time_point >> rec.process_time >> rec.address
		 >> rec.flags >> rec.orig_size >> rec.comp_size
         >> rec.method >> rec.url;

	return true;
}


bool compress::ZLogData::find(const compress::StatRec &stat, double range)
{
    // std::ofstream ofs ("/tmp/stat.out", std::ofstream::app);
	// ofs << (zlog.good() ? "log is ok" : "log is not ok") << std::endl;
	// ofs.close();

	double lower = stat.time_point - range;
	while(read())
	{

        // std::ofstream ofs ("/tmp/stat.out", std::ofstream::app);
		// ofs << "compare: " << lower << "<=" << rec.time_point << std::endl;
		// ofs << "compare: " <<  stat.url << "==" << rec.url << std::endl;
		// ofs.close();

		if((lower <= rec.time_point) && (stat.url == rec.url)) {

            // std::ofstream ofs ("/tmp/stat.out", std::ofstream::app);
			// ofs << "compared: "
			// 	<< std::fixed << std::setw(14) << std::setprecision(3) << std::setfill('0')
			// 	<< lower << "<="
			// 	<< std::fixed << std::setw(14) << std::setprecision(3) << std::setfill('0')
			// 	<< rec.time_point << std::endl;
			// ofs << "compared: " <<  stat.url << "==" << rec.url << std::endl;
			// ofs.close();

			return true;
		}
	}
	return false;
}


compress::StatRec *compress::ZLogData::find(std::list<compress::StatRec*> &stats, double range)
{
    Logger log;

    if(!zlog.good()) {
        return NULL;
	}
	if(!rec.time_point) {read();}
	do
	{
		for(std::list<StatRec*>::iterator i = stats.begin(); i != stats.end(); ++i)
        {
            if((*i)->fail_count == -1)
            {
                log.debug() << "already handled but failed to save in storage entity" << std::endl;
                continue;
            }
			if(rec.time_point >= ((*i)->time_point - range) && rec.url == (*i)->url)
			{
				StatRec *s = *i;
				s->orig_size = rec.orig_size;
                s->comp_size = rec.comp_size;
                s->host_addr = rec.address;
                stats.erase(i);
                return s;
			}
		}
    } while(read());
    return NULL;
}


inline const compress::ZLogRec &compress::ZLogData::getRec() const
{
	return rec;
}


// storing/sending statistc bursts

compress::Storage::Storage(long step, const std::string &file_prefix)
    : storage_fname(new char[file_prefix.size() + 20]), storage_threshold(0),
      threshold_step(step), sock(0)
{
    if(file_prefix.size() > postfix_size) {
        std::snprintf(storage_fname, postfix_size, "%s.", file_prefix.c_str());
        fname_postfix = storage_fname + postfix_size + 1;
    } else {
        std::sprintf(storage_fname, "%s.", file_prefix.c_str());
        fname_postfix = storage_fname + file_prefix.size() + 1;
    }
}


compress::Storage::~Storage()
{
    flush(true);
}


void compress::Storage::flush(bool close)
{
    if(storage_stream.is_open()) {
        storage_stream.flush();
        if(close) {
            storage_stream.close();
        }
    }
    for(std::list<StatBurst>::iterator i = fq.begin(); i != fq.end();)
    {
        if(send(i->name) || close) {
            i = fq.erase(i);
        }
    }
}


bool compress::Storage::add(const StatRec &stat)
{
    if(storage_threshold <= 0) {
        if(!init(Logger::timePoint(), threshold_step)) {
            return false;
        }
    } else if(storage_threshold < stat.time_point) {
        storage_stream.close();
        if(!init(stat.time_point, threshold_step)) {
            return false;
        }
    }
    if(storage_stream.bad()) {
        return false;
    }

    storage_stream << fmt(stat.time_point) << "\t" << stat.address << "\t" << stat.host_addr
                   << "\t" << stat.orig_size << "\t" << stat.comp_size << "\n";
    return storage_stream.good();
}


long compress::Storage::step() const
{
    return threshold_step;
}


void compress::Storage::step(long new_val)
{
    threshold_step = new_val;
}


bool compress::Storage::init(long start, long step)
{
    Logger log;

    storage_threshold = start + step;

    log.info() << "New storage_threshold = " << storage_threshold << std::endl;

    std::snprintf(fname_postfix, postfix_size, "%ld", storage_threshold);
    fname_postfix[postfix_size] = '\0';

    log.info() << "New storage name = '" << storage_fname << "'" << std::endl;

    storage_stream.open(storage_fname, std::ofstream::app);
    if(storage_stream.bad()){
        log.err() << "Failed to open storage file: '" << storage_fname << "'" << std::endl;
        storage_threshold = 0;
        return false;
    }
    chmod(storage_fname, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH);
    log.debug() << "Storage opened" << std::endl;

    StatBurst burst(storage_fname, start + 2 * step);
    log.debug() << "Burst: " << burst.send_time << ", " << burst.name << std::endl;

    fq.push_back(burst);
    for(std::list<StatBurst>::iterator i = fq.begin(); i != fq.end();)
    {
        if(i->send_time <= start && send(i->name)) {
            i = fq.erase(i);
        } else {
            ++i;
        }
    }
    return true;
}


bool compress::Storage::send(const std::string &fname)
{
    Logger log;
    try
    {
        sock = socket(AF_UNIX, SOCK_DGRAM, 0);
        if(sock < 0) {
            log.err() << "Error opening Unix Domain Socket" << std::endl;
            return false;
        }
        memset(&server, 0, sizeof(struct sockaddr_un));
        server.sun_family = AF_UNIX;
        strcpy(server.sun_path, agent_addr);
        if(connect(sock, (struct sockaddr *) &server, sizeof(struct sockaddr_un)) != 0)
        {
            close(sock);
            log.err() << "Error connecting to Unix Domain Socket '" << agent_addr << "'" << std::endl;
            return false;
        }
        if(write(sock, fname.c_str(), fname.size()) < 0) {
            log.err() << "Error writing to Unix Domain Socket ("
                      << __func__ << ": " << __LINE__ << ")" << std::endl;
            return false;
        }
        close(sock);
    }
    catch(std::exception &e) {
        log.err() << "Got exception while works with socket: " << e.what() << ". "
                  << __func__ << ": " << __LINE__ << std::endl;
        return false;
    }
    catch(...) {
        log.err() << "Got exception while works with socket "
                  << __func__ << ": " << __LINE__ << std::endl;
        return false;
    }
    return true;
}



// adapter service

compress::Service::Service()
    : storage(default_burst_step)
{
	pthread_mutex_init(&stats_lock, NULL);
}


compress::Service::~Service()
{
	handleStats(true);
	pthread_mutex_destroy(&stats_lock);
}


std::string compress::Service::uri() const
{
	return "ecap://privatewifi.com/ecap/services/statistic";
}


std::string compress::Service::tag() const
{
	return PACKAGE_VERSION;
}


void compress::Service::describe(std::ostream &os) const
{
    os << "HTTP traffic saving statistic adapter from " << PACKAGE_NAME << " v" << PACKAGE_VERSION;
}


void compress::Service::configure(const libecap::Options &cfg)
{
    Cfgtor cfgtor(*this);
    cfg.visitEachOption(cfgtor);
}


void compress::Service::reconfigure(const libecap::Options &cfg)
{
    configure(cfg);
}


void compress::Service::setOne(const libecap::Name &name, const libecap::Area &valArea) {
    Logger log;
    const std::string value = valArea.toString();
    if(name == "mongodb_delay") {
        if(!setDataBurstDelay(value)) {
                log.err() << "'mongodb_delay' parameter is not a valid number of minutes"
                          << ", default(" << default_burst_step << ") used instead"<< std::endl;
        } else {
            log.info() << "New 'mongodb_delay' parameter used: " << value << " minutes" << std::endl;
        }
    } else {
        // throw libecap::TextException(CfgErrorPrefix +
        //                              "unsupported configuration parameter: " + name.image());
    }
}


bool compress::Service::setDataBurstDelay(const std::string &value)
{
    long val = std::atol(value.c_str());
    if(val <= 0) {
        return false;
    }
    storage.step(val * 60);
    return true;
}


void compress::Service::start()
{
	libecap::adapter::Service::start();
}


void compress::Service::stop()
{
    storage.flush();
	libecap::adapter::Service::stop();
}


void compress::Service::retire()
{
    storage.flush();
	libecap::adapter::Service::stop();
}


bool compress::Service::wantsUrl(const char *url) const
{
	return true; // minimal adapter is applied to all messages
}


#ifdef LIBECAP_VERSION_020
libecap::adapter::Xaction *compress::Service::makeXaction(libecap::host::Xaction *hostx) {
    return new compress::Transact(
            std::tr1::static_pointer_cast<Service>(self), hostx);
}
#else
compress::Service::MadeXactionPointer compress::Service::makeXaction(
    libecap::host::Xaction *hostx)
{
    return compress::Service::MadeXactionPointer(
        new compress::Transact(
            std::tr1::static_pointer_cast<Service>(self), hostx));
}
#endif

void compress::Service::addStat(StatRec* stat)
{
	Logger log;

	pthread_mutex_lock(&stats_lock);

    log.debug() << "Add (" << fmt(stats.size()) << "): "
               << fmt(stat->time_point) << ", " << fmt(stat->url) << std::endl;

    stats.push_back(stat);

    if(stats.size() >= stats_max_count) {
		handleStats();
	}
	pthread_mutex_unlock(&stats_lock);
}


void compress::Service::handleStats(bool final)
{
    Logger log;
	ZLogData zlog(zlog_fname, stats.front()->time_point);
	StatRec *stat = NULL;
	while((stat = zlog.find(stats)))
    {
        if(storage.add(*stat)) {

            log.debug() << "Stored(" << fmt(stats.size()) << "): "
                       << fmt(stat->time_point) << ", " << fmt(stat->url) << std::endl;

            delete stat;
        } else {

            log.err() << "Fail to store(" << fmt(stats.size()) << "): "
                      << fmt(stat->time_point) << ", " << fmt(stat->url) << std::endl;

            stat->fail_count = -1;
            stats.push_back(stat);
        }
	}
	for(std::list<StatRec*>::iterator i = stats.begin(); i != stats.end();)
	{
		Logger log;
        StatRec *s = *i;
        double now_tp = Logger::timePoint();
        if(final || (s->fail_count != -1 &&
                     ++(s->fail_count) > stat_max_failure &&
                     now_tp > (s->time_point + url_alive_time)))
		{
            log.debug() << "Drop(" << fmt(stats.size()) << "): "
                       << fmt(s->time_point) << ", " << fmt(s->url) << std::endl;
			i = stats.erase(i);
			delete s;
		} else {
			++i;
		}
	}
}


void compress::Service::handleStatsBack(bool final)
{
	Logger log;
	for(std::list<StatRec*>::iterator i = stats.begin(); i != stats.end();)
    {
        if((*i)->fail_count == -1) {continue;} // already handles but failed to save in storage entity
        StatRec *s = *i;
		ZLogData zlog(zlog_fname);
        if(zlog.find(*s)) {
            const ZLogRec &rec = zlog.getRec();
            s->orig_size = rec.orig_size;
            s->comp_size = rec.comp_size;
            s->host_addr = rec.address;

            storage.add(*s);
			i = stats.erase(i);
			delete s;
		} else {
			if(final || ++(s->fail_count) > stat_max_failure) {
                log.debug() << "Drop(" << fmt(stats.size()) << "): "
                           << fmt(s->time_point) << ", " << fmt(s->url) << std::endl;

				i = stats.erase(i);
				delete s;
			} else {
				++i;
			}
		}
	}
}



// transaction

compress::Transact::Transact(libecap::shared_ptr<Service> aService,
                             libecap::host::Xaction *x)
	: service(aService), hostx(x), reqmode(true), body_size(0)
{
}


compress::Transact::~Transact()
{
	if (libecap::host::Xaction *x = hostx) {
		hostx = 0;
		x->adaptationAborted();
	}
}


const libecap::Area compress::Transact::option(const libecap::Name &) const
{
	return libecap::Area(); // this transaction has no meta-information
}


void compress::Transact::visitEachOption(libecap::NamedValueVisitor &) const
{
	// this transaction has no meta-information to pass to the visitor
}


void compress::Transact::start()
{
	Must(hostx);

	bool respod_only = true;

	//bool body_exists = false;
	//if (hostx->virgin().body()) {
    //body_exists = true;
    //hostx->vbMake(); // ask host to supply virgin body
    // const libecap::Area vb = hostx->vbContent(0, libecap::nsize);
    // body_size = vb.size;
    //if(hostx->virgin().body()->bodySize().known()){
    //	body_size = 2;//hostx->virgin().body()->bodySize().value();
    //} else {
    //	body_size = 1;
    //}
	//}

	libecap::Area uri;
	libecap::Name method;
    typedef const libecap::RequestLine *CLRLP;
    if(CLRLP request_line = dynamic_cast<CLRLP>(&hostx->virgin().firstLine())) {
		reqmode = true;
		uri = request_line->uri();
		method = request_line->method();

		if(respod_only) {
			hostx->useVirgin();
			return;
		}
    } else {
		// looks like not a reqmode so check respmod method
		reqmode = false;
		if(CLRLP request_line = dynamic_cast<CLRLP>(&hostx->cause().firstLine())) {
            uri = request_line->uri();
			method = request_line->method();
		}
	}
	libecap::Area client_ip = hostx->option(libecap::metaClientIp);
	StatRec *cur = new StatRec;
	cur->time_point = Logger::timePoint();
	cur->url.assign(uri.start, uri.size);
	cur->method = method.image();
	cur->address.assign(client_ip.start, client_ip.size);
	// static const libecap::Name useragent_name("User-Agent");
	// libecap::Header::Value agent = hostx->virgin().header().value(useragent_name);
	service->addStat(cur);

	hostx->useVirgin();
}


void compress::Transact::stop()
{
	// std::ofstream ofs (storage_fname, std::ofstream::app);
	// ofs << std::fixed << std::setw(14) << std::setprecision(3) << std::setfill('0') << timePoint()
	// 	<< "\tTransaction stoped\n";
	// ofs.close();

	hostx = 0;
	// the caller will delete
}


#ifdef LIBECAP_VERSION_020
bool compress::Transact::callable() const
{
    return hostx != 0;
}
#endif



// create the adapter and register with libecap to reach the host application
#ifdef LIBECAP_VERSION_020
static const bool Registered = (libecap::RegisterService(new compress::Service), true);
#else
static const bool Registered =
    libecap::RegisterVersionedService(new compress::Service);
#endif
