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
#include "actor/actor_validation_object.hpp"
#include "core/abstract_mailbox.hpp"
#include "base/type.hpp"
#include "utils/tool.hpp"

class ProjectActor : public AbstractActor {
 public:
    ProjectActor(int id,
            int machine_id,
            int num_thread,
            AbstractMailbox * mailbox,
            CoreAffinity* core_affinity) :
        AbstractActor(id, core_affinity),
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

        if (qplan.trx_type != TRX_READONLY) {
            // Record Input Set
            for (auto & data_pair : msg.data) {
                v_obj.RecordInputSetValueT(qplan.trxid, actor_obj.index, inType, data_pair.second, m.step == 1 ? true : false);
            }
        }

        // get projection function acccording to element type
        bool (ProjectActor::*proj)(const QueryPlan&, vector<value_t>&, int, int, map<value_t, vector<value_t>>&);
        switch (inType) {
          case Element_T::VERTEX: proj = &this->project_vertex; break;
          case Element_T::EDGE:   proj = &this->project_edge; break;
          default:
            cout << "Wrong element type in project actor!" << endl;
            assert(false);
        }
        vector<pair<history_t, vector<value_t>>> newData;
        bool read_success = true;
        for (auto & pair : msg.data) {
            map<value_t, vector<value_t>> proj_map;
            // Project E/V to property
            if (!(this->*proj)(qplan, pair.second, key_id, value_id, proj_map)) {
                read_success = false;
                break;
            }
            // Insert projected kv pair
            for (auto itr = proj_map.begin(); itr != proj_map.end(); itr++) {
                history_t his = pair.first;
                // Add key to history
                his.emplace_back(m.step, move(itr->first));
                newData.emplace_back(move(his), move(itr->second));
            }
        }

        vector<Message> msg_vec;
        if (read_success) {
            msg.CreateNextMsg(qplan.actors, newData, num_thread_, core_affinity_, msg_vec);
        } else {
            string abort_info = "Abort with [Processing][ProjectActor::process]";
            msg.CreateAbortMsg(qplan.actors, msg_vec, abort_info);
        }

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
                if (!v_obj.Validate(TrxID, actor_obj->index, local_check_set)) {
                    return false;
                }
            }
        }
        return true;
    }

    void clean_trx_data(uint64_t TrxID) { v_obj.DeleteInputSet(TrxID); }

 private:
    // Number of threads
    int num_thread_;
    int machine_id_;

    // Actor type
    ACTOR_T type_;

    // Pointer of mailbox
    AbstractMailbox * mailbox_;

    Config* config_;

    // Validation Store
    ActorValidationObject v_obj;

    READ_STAT get_properties_for_vertex(const QueryPlan& qplan, const vpid_t& vp_id, value_t& val) {
        if (vp_id.pid == 0) {
            vid_t vid(vp_id.vid);
            label_t label;
            READ_STAT read_status = data_storage_->GetVL(vid, qplan.trxid, qplan.st, qplan.trx_type == TRX_READONLY, label);
            if (read_status == READ_STAT::SUCCESS) {
                string label_str;
                data_storage_->GetNameFromIndex(Index_T::V_LABEL, label, label_str);
                Tool::str2str(label_str, val);
            }
            return read_status;
        } else {
            return data_storage_->GetVPByPKey(vp_id, qplan.trxid, qplan.st, qplan.trx_type == TRX_READONLY, val);
        }
    }

    bool project_vertex(const QueryPlan& qplan, vector<value_t>& data, int key_id, int value_id,
                        map<value_t, vector<value_t>>& proj_map) {
        for (auto & val : data) {
            vid_t v_id(Tool::value_t2int(val));
            value_t key, value;

            // project key
            vpid_t vp_id(v_id, key_id);
            READ_STAT read_status = get_properties_for_vertex(qplan, vp_id, key);
            switch (read_status) {
              case READ_STAT::SUCCESS: break;
              case READ_STAT::NOTFOUND: continue;
              case READ_STAT::ABORT: return false;
              default : cout << "[Error] Unexpected READ_STAT in ProjectActor" << endl;
            }

            if (value_id == -1) {
                // no value projection, keep origin value
                value = move(val);
            } else {
                vp_id.pid = value_id;
                READ_STAT read_status = get_properties_for_vertex(qplan, vp_id, value);
                switch (read_status) {
                  case READ_STAT::SUCCESS: break;
                  case READ_STAT::NOTFOUND: continue;
                  case READ_STAT::ABORT: return false;
                  default : cout << "[Error] Unexpected READ_STAT in ProjectActor" << endl;
                }
            }

            proj_map[key].push_back(value);
        }
        return true;
    }

    READ_STAT get_properties_for_edge(const QueryPlan& qplan, const epid_t& ep_id, value_t& val) {
        if (ep_id.pid == 0) {
            eid_t eid(ep_id.in_vid, ep_id.out_vid);
            label_t label;
            READ_STAT read_status = data_storage_->GetEL(eid, qplan.trxid, qplan.st, qplan.trx_type == TRX_READONLY, label);
            if (read_status == READ_STAT::SUCCESS) {
                string label_str;
                data_storage_->GetNameFromIndex(Index_T::E_LABEL, label, label_str);
                Tool::str2str(label_str, val);
            }
            return read_status;
        } else {
            return data_storage_->GetEPByPKey(ep_id, qplan.trxid, qplan.st, qplan.trx_type == TRX_READONLY, val);
        }
    }

    bool project_edge(const QueryPlan& qplan, vector<value_t>& data, int key_id, int value_id,
                      map<value_t, vector<value_t>>& proj_map) {
        for (auto & val : data) {
            eid_t e_id;
            uint2eid_t(Tool::value_t2uint64_t(val), e_id);

            value_t key, value;
            // project key
            epid_t ep_id(e_id, key_id);
            READ_STAT read_status = get_properties_for_edge(qplan, ep_id, key);
            switch (read_status) {
              case READ_STAT::SUCCESS: break;
              case READ_STAT::NOTFOUND: continue;
              case READ_STAT::ABORT: return false;
              default : cout << "[Error] Unexpected READ_STAT in ProjectActor" << endl;
            }

            if (value_id == -1) {
                // no value projection, keep origin value
                value = move(val);
            } else {
                ep_id.pid = value_id;
                READ_STAT read_status = get_properties_for_edge(qplan, ep_id, value);
                switch (read_status) {
                  case READ_STAT::SUCCESS: break;
                  case READ_STAT::NOTFOUND: continue;
                  case READ_STAT::ABORT: return false;
                  default : cout << "[Error] Unexpected READ_STAT in ProjectActor" << endl;
                }
            }

            proj_map[key].push_back(value);
        }
        return true;
    }
};

#endif  // ACTOR_PROJECT_ACTOR_HPP_
