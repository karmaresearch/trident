#ifndef __MEMORY_FILE_H
#define __MEMORY_FILE_H

#include <kognac/utils.h>
#include <kognac/logs.h>

#include <string>

#include <fcntl.h>
#if defined(_WIN32)
#define NOMINMAX
#include <windows.h>
#include <stdio.h>
#include <conio.h>
#include <tchar.h>
#elif defined(__unix__) || defined(__unix) || defined(unix) || (defined(__APPLE__) && defined(__MACH__))
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>
#endif

class MemoryMappedFile {
    private: 
#if defined(_WIN32)
		HANDLE fd;
		HANDLE mappedfd;

#elif defined(__unix__) || defined(__unix) || defined(unix) || (defined(__APPLE__) && defined(__MACH__))
		int fd;
#endif
		size_t length;
		char *data;

    public:
        MemoryMappedFile(std::string file, bool ro, off_t start,
                size_t len) {
#if defined(_WIN32)
			fd = CreateFile(file.c_str(), ro ? GENERIC_READ : GENERIC_READ | GENERIC_WRITE, FILE_SHARE_READ, NULL, OPEN_ALWAYS, FILE_ATTRIBUTE_NORMAL, NULL);
			LARGE_INTEGER size;
			GetFileSizeEx(fd, &size);
			mappedfd = CreateFileMapping(fd, NULL, ro ? PAGE_READONLY : PAGE_READWRITE, 0, 0, NULL);
			if (!mappedfd) {
				std::string error;
				DWORD errorMessageID = ::GetLastError();
				if (errorMessageID != 0) {
					LPSTR messageBuffer = nullptr;
					size_t size = FormatMessageA(FORMAT_MESSAGE_ALLOCATE_BUFFER | FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS,
						NULL, errorMessageID, MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT), (LPSTR)&messageBuffer, 0, NULL);
					error = string(messageBuffer, size);
					LocalFree(messageBuffer);
				}
				LOG(ERRORL) << "Error mapping file " << file << " Error: " << error;
			}
			data = (char *)MapViewOfFileEx(mappedfd, ro ? FILE_MAP_READ : FILE_MAP_ALL_ACCESS, HIWORD(start), LOWORD(start), len, NULL);
#elif defined(__unix__) || defined(__unix) || defined(unix) || (defined(__APPLE__) && defined(__MACH__))
            fd = ::open(file.c_str(), ro ?  O_RDONLY : (O_RDWR | O_CREAT),
		    ro ? S_IREAD : (S_IREAD | S_IWRITE));
            if (fd == -1) {
                LOG(ERRORL) << "Failed opening the file " << file;
                throw 10;
            } 
            data = static_cast<char*>(::mmap(NULL, len,
                        ro ? PROT_READ : (PROT_READ | PROT_WRITE),
                        MAP_SHARED, fd, start));
            if (data == MAP_FAILED) {
                LOG(ERRORL) << "Failed mapping the file " << file;
                throw 10;
            }
#endif
			this->length = len;
        }

        MemoryMappedFile(std::string file, bool ro) : MemoryMappedFile(file, ro, 0,
                (int64_t)Utils::fileSize(file)) {
        }

        MemoryMappedFile(std::string file) : MemoryMappedFile(file, true, 0,
                (int64_t)Utils::fileSize(file)) {
        }

        ~MemoryMappedFile() {
#if defined(_WIN32)
			UnmapViewOfFile(mappedfd);
			CloseHandle(fd);
#elif defined(__unix__) || defined(__unix) || defined(unix) || (defined(__APPLE__) && defined(__MACH__))
            munmap(const_cast<char*>(data), length);
            close(fd);
#endif
        }

        char *getData() {
            return data;
        }

        size_t getLength() {
            return length;
        }

		void flushAll() {
#if defined(_WIN32)
			if (!FlushViewOfFile(data, length)) {
				LOG(ERRORL) << "Flushing file returned an error!";
			}
#elif defined(__unix__) || defined(__unix) || defined(unix) || (defined(__APPLE__) && defined(__MACH__))
            msync(data, length, MS_SYNC);
#endif
        }

        static int alignment() {
#if defined(_WIN32)
			LOG(ERRORL) << "alignment mapped file is not implemented";
			throw 10;
#elif defined(__unix__) || defined(__unix) || defined(unix) || (defined(__APPLE__) && defined(__MACH__))
			return static_cast<int>(sysconf(_SC_PAGESIZE));
#endif
        }

        void flush(off_t begin, size_t len) {
#if defined(_WIN32)
			if (!FlushViewOfFile(data + begin, len)) {
				LOG(ERRORL) << "Flushing file returned an error!";
			}
#elif defined(__unix__) || defined(__unix) || defined(unix) || (defined(__APPLE__) && defined(__MACH__))
            msync(data + begin, len, MS_SYNC);
#endif
        }
};

#endif
