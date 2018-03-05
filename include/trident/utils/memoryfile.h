#ifndef __MEMORY_FILE_H
#define __MEMORY_FILE_H

#include <kognac/utils.h>
#include <kognac/logs.h>

#include <string>

#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>

class MemoryMappedFile {
    private:
        int fd;
        size_t length;
        char *data;

    public:
        MemoryMappedFile(std::string file, bool ro, off_t start,
                size_t len) {
            fd = ::open(file.c_str(), ro ?  O_RDONLY : (O_RDWR | O_CREAT),
		    ro ? S_IREAD : (S_IREAD | S_IWRITE));
            if (fd == -1) {
                LOG(ERRORL) << "Failed opening the file " << file;
                throw 10;
            }
            this->length = len;
            data = static_cast<char*>(::mmap(NULL, len,
                        ro ? PROT_READ : (PROT_READ | PROT_WRITE),
                        MAP_SHARED, fd, start));
            if (data == MAP_FAILED) {
                LOG(ERRORL) << "Failed mapping the file " << file;
                throw 10;
            }
        }

        MemoryMappedFile(std::string file, bool ro) : MemoryMappedFile(file, ro, 0,
                Utils::fileSize(file)) {
        }

        MemoryMappedFile(std::string file) : MemoryMappedFile(file, true, 0,
                Utils::fileSize(file)) {
        }

        ~MemoryMappedFile() {
            munmap(const_cast<char*>(data), length);
            close(fd);
        }

        char *getData() {
            return data;
        }

        size_t getLength() {
            return length;
        }

        void flushAll() {
            msync(data, length, MS_SYNC);
        }

        static int alignment() {
            return static_cast<int>(sysconf(_SC_PAGESIZE));
        }

        void flush(off_t begin, size_t len) {
            msync(data + begin, len, MS_SYNC);
        }
};

#endif
