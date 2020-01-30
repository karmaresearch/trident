#include <jni.h>
#include <iostream>

#include <trident/server/server.h>
#include <trident/utils/httpserver.h>
#include <trident/utils/json.h>
#include <trident/kb/kb.h>
#include <trident/kb/querier.h>
#include <trident/sparql/sparql.h>
#include <kognac/logs.h>

jlong getId(std::string nameField, JNIEnv *env, jobject &jobj) {
    jclass vlogcls = env->GetObjectClass(jobj);
    jfieldID fid = env->GetFieldID(vlogcls, nameField.c_str(), "J");
    return env->GetLongField(jobj, fid);
}

void setId(std::string nameField, JNIEnv *env, jobject &jobj, jlong id) {
    jclass vlogcls = env->GetObjectClass(jobj);
    jfieldID fid = env->GetFieldID(vlogcls, nameField.c_str(), "J");
    env->SetLongField(jobj, fid, id);
}

long nterms;

#ifdef __cplusplus
extern "C" {
#endif
    JNIEXPORT void JNICALL Java_karmaresearch_trident_Trident_load
        (JNIEnv *env, jobject obj, jstring jpath) {
            //Set level to warning
            Logger::setMinLevel(WARNL);
            KBConfig config;
            std::vector<string> locUpdates;
            const char *path = env->GetStringUTFChars(jpath, 0);
            auto kb = new KB(path, true, false, true, config, locUpdates);
            TridentLayer *tlayer = new TridentLayer(*kb);
            nterms = kb->getNTerms();

            setId("myTrident", env, obj, (jlong)kb);
            setId("myTridentLayer", env, obj, (jlong)tlayer);

            std::cout << "Trident database is loaded." << std::endl;

            env->ReleaseStringUTFChars(jpath, path);
        }

    JNIEXPORT jstring JNICALL Java_karmaresearch_trident_Trident_sparql
        (JNIEnv *jenv, jobject jobj, jstring jquery) {
            auto id = getId("myTridentLayer", jenv, jobj);
            TridentLayer *db = (TridentLayer*)id;

            //Execute the sparql query
            JSON pt;
            JSON vars;
            JSON bindings;

            JSON stats;
            const char *query = jenv->GetStringUTFChars(jquery, 0);
            SPARQLUtils::execSPARQLQuery(query,
                    false,
                    nterms,
                    *db,
                    false,
                    true,
                    &vars,
                    &bindings,
                    &stats);
            jenv->ReleaseStringUTFChars(jquery, query);
            JSON head;
            head.add_child("vars", vars);
            pt.add_child("head", head);
            JSON results;
            results.add_child("bindings", bindings);
            pt.add_child("results", results);
            pt.add_child("stats", stats);

            std::ostringstream buf;
            JSON::write(buf, pt);
            auto page = buf.str();
            //Return the results
            auto out = jenv->NewStringUTF(page.c_str());
            return out;
        }

    JNIEXPORT void JNICALL Java_karmaresearch_trident_Trident_unload
        (JNIEnv *env, jobject obj) {
            auto idl = getId("myTridentLayer", env, obj);
            TridentLayer *address1 = (TridentLayer*)idl;
            delete address1;
            auto id = getId("myTrident", env, obj);
            KB *address = (KB*)id;
            delete address;
            std::cout << "Trident database is unloaded." << std::endl;
        }

#ifdef __cplusplus
}
#endif
