#pragma once
#include <cstdint>
#include <string>

#include "common/slice.h"
#include "common/status.h"

namespace tinydb {

class File;

enum FileType {
  kLogFile,
  kDBLockFile,
  kTableFile,
  kDescriptorFile,
  kCurrentFile,
  kTempFile,
  kInfoLogFile // Either the current one, or an old one
};

// Return the name of the log file with the specified number
// in the db named by "dbname".  The result will be prefixed with
// "dbname".
std::string LogFileName(const std::string& dbname, uint64_t number);

// Return the name of the sstable with the specified number
// in the db named by "dbname". The result will be prefixed with
// "dbname".
std::string SSTTableFileName(const std::string& dbname, uint64_t number);

// Return the name of the descriptor file for the db named by
// "dbname" and the specified incarnation number.  The result will be
// prefixed with "dbname".
std::string DescriptorFileName(const std::string& dbname, uint64_t number);

// Return the name of the current file.  This file contains the name
// of the current manifest file.  The result will be prefixed with
// "dbname".
std::string CurrentFileName(const std::string& dbname);

// Return the name of the lock file for the db named by
// "dbname".  The result will be prefixed with "dbname".
std::string LockFileName(const std::string& dbname);

// Return the name of a temporary file owned by the db named "dbname".
// The result will be prefixed with "dbname".
std::string TempFileName(const std::string& dbname, uint64_t number);

// Return the name of the info log file for "dbname".
std::string InfoLogFileName(const std::string& dbname);

// Return the name of the old info log file for "dbname".
std::string OldInfoLogFileName(const std::string& dbname);

// If filename is a tinydb file, store the type of the file in *type.
// The number encoded in the filename is stored in *number.
bool ParseFileName(const std::string& filename, uint64_t* number,
                   FileType* type);

// Make the CURRENT file point to the descriptor file with the
// specified number.
Status SetCurrentFile(File* file, const std::string& dbname,
                      uint64_t descriptor_number);

} // namespace tinydb
