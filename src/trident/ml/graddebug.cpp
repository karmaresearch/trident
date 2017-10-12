#include <trident/ml/graddebug.h>
#include <trident/utils/json.h>

#include <kognac/logs.h>

#include <zstr/zstr.hpp>

void GradTracer::add(uint16_t epoch, uint32_t id,
        std::vector<float> &features,
        uint32_t n) {
    _Update up;
    up.epoch = epoch;
    up.n = n;
    up.startidx = updates_values[id].size();
    //Add the values
    if (all || s_entities.count(id)) {
        for(uint16_t i = 0; i < dim; ++i) {
            updates_values[id].push_back(features[i]);
        }
    }
    updates_meta[id].push_back(up);
}

void GradTracer::store(std::string file) {
    LOG(DEBUGL) << "Storing the debug info about the gradients in " << file;
    //Write down the content as a JSON file
    JSON pt;
    pt.put("nents", std::to_string(updates_meta.size()));
    JSON traces;

    for(uint32_t idx = 0; idx < updates_meta.size(); ++idx) {
        uint32_t entityId = idx;
        LOG(DEBUGL) << "Process entity " << entityId << " ...";
        auto trace = updates_meta[entityId];
        JSON enttraces;
        for (auto t : trace) {
            JSON pent;
            pent.put("epoch", t.epoch);
            pent.put("n", t.n);
            pent.put("startidx", t.startidx);
            if (s_entities.count(entityId)) {
                JSON grad;
                for(uint16_t j = 0; j < dim; ++j) {
                    grad.push_back(std::to_string(updates_values[entityId][t.startidx + j]));
                }
                pent.add_child("gradient", grad);
            }
            enttraces.push_back(pent);
        }
        JSON ent;
        ent.put("entity", std::to_string(entityId));
        ent.add_child("trace", enttraces);
        traces.push_back(ent);
    }
    pt.add_child("traces", traces);

    //Store in gzip format
    zstr::ofstream ofs(file);
    JSON::write(ofs, pt);
}
