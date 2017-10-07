#ifndef _METH_H
#define _METH_H

#include <trident/utils/tridentutils.h>

#include <string>
#include <vector>
#include <map>

using namespace std;

class AnalyticsTasks {
    public:
        typedef enum e_type { DOUBLE, INT, LONG, BOOL, PATH } TYPE;

        struct Param {
            const string name;
            const TYPE type;
            const string defvalue;
            string value;

            Param(string name, TYPE type, string defvalue) : name(name), type(type), defvalue(defvalue), value("") {
            }

            void set(string value);

            string getRawValue() {
                if (value == "")
                    return defvalue;
                else
                    return value;
            }

            template<typename K>
                K as() {
                    return TridentUtils::lexical_cast<K>(getRawValue());
                }
        };

        struct Task {
            const string name;
            vector<Param> params;

            Task(string name, vector<Param> params) : name(name), params(params) {}

            void updateParam(string nameparam, string valueparam);

            Param &getParam(string nameparam);

            string tostring();
        };

        bool isInitialized() {
            return init;
        }

    private:
        static AnalyticsTasks __tasks_;

        std::map<string, Task> tasks;
        bool init;

        void initialize();

        string getStringType(TYPE t);

    public:
        static AnalyticsTasks &getInstance() {
            if (!AnalyticsTasks::__tasks_.isInitialized()) {
                AnalyticsTasks::__tasks_.initialize();
            }
            return AnalyticsTasks::__tasks_;
        }

        AnalyticsTasks() {
            init = false;
        }

        std::map<string, Task> getTasks() {
            return tasks;
        }

        Task getTask(string name) {
            return tasks.find(name)->second;
        }

        bool isValidTask(string name) {
            return tasks.find(name) != tasks.end();
        }

        string getTaskDetails();

        void load(string nametask, string raw);
};
#endif
