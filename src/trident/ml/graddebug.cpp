#include <trident/ml/graddebug.h>

#include <kognac/logs.h>

#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <boost/iostreams/filter/gzip.hpp>
#include <boost/iostreams/filtering_stream.hpp>

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
    boost::property_tree::ptree pt;
    pt.put("nents", std::to_string(updates_meta.size()));
    boost::property_tree::ptree traces;

    for(uint32_t idx = 0; idx < updates_meta.size(); ++idx) {
        uint32_t entityId = idx;
        LOG(DEBUGL) << "Process entity " << entityId << " ...";
        auto trace = updates_meta[entityId];
        boost::property_tree::ptree enttraces;
        for (auto t : trace) {
            boost::property_tree::ptree pent;
            pent.put("epoch", t.epoch);
            pent.put("n", t.n);
            pent.put("startidx", t.startidx);
            if (s_entities.count(entityId)) {
                boost::property_tree::ptree grad;
                for(uint16_t j = 0; j < dim; ++j) {
                    boost::property_tree::ptree el;
                    el.put("", std::to_string(updates_values[entityId][t.startidx + j]));
                    grad.push_back(std::make_pair("", el));
                }
                pent.add_child("gradient", grad);
            }
            enttraces.push_back(std::make_pair("", pent));
        }
        boost::property_tree::ptree ent;
        ent.put("entity", std::to_string(entityId));
        ent.add_child("trace", enttraces);
        traces.push_back(std::make_pair("", ent));
    }
    pt.add_child("traces", traces);

    //Store in gzip format
    std::ofstream ofs(file);
    boost::iostreams::filtering_stream<boost::iostreams::output> out;
    out.push(boost::iostreams::gzip_compressor());
    out.push(ofs);
    write_json(out, pt, false);
}
