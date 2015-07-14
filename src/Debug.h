#ifndef ECAP_ADAPTER_SAMPLE_DEBUG_H
#define ECAP_ADAPTER_SAMPLE_DEBUG_H

#include <libecap/common/log.h>
#include <iosfwd>

using libecap::ilNormal;
using libecap::ilCritical;
using libecap::flXaction;
using libecap::flApplication;
using libecap::mslLarge;

// libecap::host::openDebug/closeDebug calls wrapper for safety and convenience
class Debug {
	public:
		static std::string Prefix; // optional log line prefix

		explicit Debug(const libecap::LogVerbosity lv); // opens
		~Debug(); // closes

		// logs a message if host enabled debugging at the specified level
		template <class T>
		const Debug &operator <<(const T &msg) const {
			if (debug)
				*debug << msg;
			return *this;
		}

	private:
		/* prohibited and not implemented */
		Debug(const Debug&);
		Debug &operator=(const Debug&);

		std::ostream *debug; // host-provided debug ostream or nil
};

#endif /* ECAP_ADAPTER_SAMPLE_DEBUG_H */
