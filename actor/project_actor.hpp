/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Nick Fang (jcfang6@cse.cuhk.edu.hk)
*/

#ifndef ACTOR_PROJECT_ACTOR_HPP_
#define ACTOR_PROJECT_ACTOR_HPP_

#include <string>
#include <utility>
#include <vector>
#include <map>

#include "actor/abstract_actor.hpp"
#include "actor/actor_cache.hpp"
#include "actor/actor_validation_object.hpp"
#include "core/message.hpp"
#include "core/abstract_mailbox.hpp"
#include "base/type.hpp"
#include "storage/layout.hpp"
#include "storage/data_store.hpp"
#include "utils/tool.hpp"
#include "utils/timer.hpp"

class ProjectActor : public AbstractActor {
 public:
    ProjectActor(int id,
            DataStore* data_store,
            int machine_id,
            int num_thread,
            AbstractMailbox * mailbox,
            CoreAffinity* core_affinity) :
        AbstractActor(id, data_store, core_affinity),
        machine_id_(machine_id),
        num_thread_(num_thread),
        mailbox_(mailbox),
        type_(ACTOR_T::PROJECT) {
        config_ = Config::GetInstance();
    }

    // inType, key_projection, value_projection
    void process(const QueryPlan & qplan, Message & msg) {
        int tid = TidMapper::GetInstance()->GetTid();

        Meta & m = msg.meta;
        Actor_Object actor_obj = qplan.actors[m.step];

        Element_T inType = (Element_T)Tool::value_t2int(actor_obj.params.at(0));
        int key_id, value_id;
        key_id = Tool::value_t2int(actor_obj.params.at(1));
        value_id = Tool::value_t2int(actor_obj.params.at(2));

        // Record Input Set
        for (auto & data_pair : msg.data) {
            v_obj.RecordInputSetValueT(qplan.trxid, actor_obj.index, inType, data_pair.second, m.step == 1 ? true : false);
        }

        // get projection function acccording to element type
        void (ProjectActor::*proj)(int, vector<value_t>&, int, int, map<value_t, vector<value_t>>&);
        switch (inType) {
          case Element_T::VERTEX: proj = &this->project_vertex; break;
          case Element_T::EDGE:   proj = &this->project_edge; break;
          default:
            cout << "Wrong element type in project actor!" << endl;
            assert(false);
        }
        vector<pair<history_t, vector<value_t>>> newData;
        for (auto & pair : msg.data) {
            map<value_t, vector<value_t>> proj_map;
            // Project E/V to property
            (this->*proj)(tid, pair.second, key_id, value_id, proj_map);
            // Insert projected kv pair
            for (auto itr = proj_map.begin(); itr != proj_map.end(); itr++) {
                history_t his = pair.first;
                // Add key to history
                his.emplace_back(m.step, move(itr->first));
                newData.emplace_back(move(his), move(itr->second));
            }
        }

        vector<Message> msg_vec;
        msg.CreateNextMsg(qplan.actors, newData, num_thread_, data_store_, core_affinity_, msg_vec);

        // Send Message
        for (auto& msg : msg_vec) {
            mailbox_->Send(tid, msg);
        }
    }

    bool valid(uint64_t TrxID, vector<Actor_Object*> & actor_list, const vector<rct_extract_data_t> & check_set) {
        for (auto & actor_obj : actor_list) {
            assert(actor_obj->actor_type == ACTOR_T::PROJECT);
            vector<uint64_t> local_check_set;

            // Analysis params
            Element_T inType = (Element_T)Tool::value_t2int(actor_obj->params.at(0));
            int key_id, value_id;
            key_id = Tool::value_t2int(actor_obj->params.at(1));
            value_id = Tool::value_t2int(actor_obj->params.at(2));

            // Compare check_set and parameters
            for (auto & val : check_set) {
                if ((get<1>(val) == key_id || get<1>(val) == value_id) && get<2>(val) == inType) {
                    local_check_set.emplace_back(get<0>(val)); 
                }
            }

            if (local_check_set.size() != 0) {
                if(!v_obj.Validate(TrxID, actor_obj->index, local_check_set)) {
                    return false;
                }
            }
        }
        return true;
    }

 private:
    // Number of threads
    int num_thread_;
    int machine_id_;

    // Actor type
    ACTOR_T type_;

    // Pointer of mailbox
    AbstractMailbox * mailbox_;

    // Cache
    ActorCache cache;
    Config* config_;

    // Validation Store
    ActorValidationObject v_obj;

    bool get_properties_for_vertex(int tid, const Vertex* vtx, int pid, value_t& val) {
        if (find(vtx->vp_list.begin(), vtx->vp_list.end(), pid) == vtx->vp_list.end()) {
            return false;
        }

        vpid_t vp_id(vtx->id, pid);
        // Try cache
        if (data_store_->VPKeyIsLocal(vp_id) || !config_->global_enable_caching) {
            data_store_->GetPropertyForVertex(tid, vp_id, val);
        } else {
            if (!cache.get_property_from_cache(vp_id.value(), val)) {
                // not found in cache
                data_store_->GetPropertyForVertex(tid, vp_id, val);
                cache.insert_properties(vp_id.value(), val);
            }
        }
        return true;
    }

    void project_vertex(int tid, vector<value_t>& data, int key_id, int value_id,
                        map<value_t, vector<value_t>>& proj_map) {
        for (auto & val : data) {
            vid_t v_id(Tool::value_t2int(val));
            Vertex* vtx = data_store_->GetVertex(v_id);
            value_t key, value;

            // project key
            if (!get_properties_for_vertex(tid, vtx, key_id, key)) {
                continue;
            }

            if (value_id == -1) {
                // no value projection, keep origin value
                value = move(val);
            } else if (!get_properties_for_vertex(tid, vtx, value_id, value)) {
                continue;
            }

            proj_map[key].push_back(value);
        }
    }

    bool get_properties_for_edge(int tid, const Edge* edge, int pid, value_t& val) {
        if (find(edge->ep_list.begin(), edge->ep_list.end(), pid) == edge->ep_list.end()) {
            return false;
        }

        epid_t ep_id(edge->id, pid);
        if (data_store_->EPKeyIsLocal(ep_id) || !config_->global_enable_caching) {
            data_store_->GetPropertyForEdge(tid, ep_id, val);
        } else {
            if (!cache.get_property_from_cache(ep_id.value(), val)) {
                // not found in cache
                data_store_->GetPropertyForEdge(tid, ep_id, val);
                cache.insert_properties(ep_id.value(), val);
            }
        }
        return true;
    }

    void project_edge(int tid, vector<value_t>& data, int key_id, int value_id,
                      map<value_t, vector<value_t>>& proj_map) {
        for (auto & val : data) {
            eid_t e_id;
            uint2eid_t(Tool::value_t2uint64_t(val), e_id);
            Edge* edge = data_store_->GetEdge(e_id);

            value_t key, value;
            // project key
            if (!get_properties_for_edge(tid, edge, key_id, key)) {
                continue;
            }

            if (value_id == -1) {
                // no value projection, keep origin value
                value = move(val);
            } else if (!get_properties_for_edge(tid, edge, value_id, value)) {
                continue;
            }

            proj_map[key].push_back(value);
        }
    }
};

#endif  // ACTOR_PROJECT_ACTOR_HPP_
