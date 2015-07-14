#include "sample.h"
#include "Debug.h"
#include <libecap/common/registry.h>
#include <libecap/host/host.h>
#include <string>
#include <iostream>

std::string Debug::Prefix;

Debug::Debug(const libecap::LogVerbosity lv):
	debug(libecap::MyHost().openDebug(lv)) {
	*debug << Prefix;
}

Debug::~Debug() {
	if (debug)
		libecap::MyHost().closeDebug(debug);
}
