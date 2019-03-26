/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Aaron Li (cjli@cse.cuhk.edu.hk)
*/

#ifndef ACTOR_PROPERTIES_ACTOR_HPP_
#define ACTOR_PROPERTIES_ACTOR_HPP_

#include <string>
#include <utility>
#include <vector>

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

class PropertiesActor : public AbstractActor {
 public:
    PropertiesActor(int id,
            DataStore* data_store,
            int machine_id,
            int num_thread,
            AbstractMailbox * mailbox,
            CoreAffinity* core_affinity) :
        AbstractActor(id, data_store, core_affinity),
        machine_id_(machine_id),
        num_thread_(num_thread),
        mailbox_(mailbox),
        type_(ACTOR_T::PROPERTY) {
        config_ = Config::GetInstance();
    }

    // inType, [key]+
    void process(const QueryPlan & qplan, Message & msg) {
        int tid = TidMapper::GetInstance()->GetTid();

        Meta & m = msg.meta;
        Actor_Object actor_obj = qplan.actors[m.step];

        Element_T inType = (Element_T)Tool::value_t2int(actor_obj.params.at(0));
        vector<int> key_list;
        for (int cnt = 1; cnt < actor_obj.params.size(); cnt++) {
            key_list.push_back(Tool::value_t2int(actor_obj.params.at(cnt)));
        }

        // Record Input Set
        for (auto & data_pair : msg.data) {
            v_obj.RecordInputSetValueT(qplan.trxid, actor_obj.index, inType, data_pair.second, m.step == 1 ? true : false);
        }

        switch (inType) {
          case Element_T::VERTEX:
            get_properties_for_vertex(qplan, tid, key_list, msg.data);
            break;
          case Element_T::EDGE:
            get_properties_for_edge(qplan, tid, key_list, msg.data);
            break;
          default:
                cout << "Wrong in type" << endl;
        }

        vector<Message> msg_vec;
        msg.CreateNextMsg(qplan.actors, msg.data, num_thread_, data_store_, core_affinity_, msg_vec);

        // Send Message
        for (auto& msg : msg_vec) {
            mailbox_->Send(tid, msg);
        }
    }

    bool valid(uint64_t TrxID, vector<Actor_Object*> & actor_list, const vector<rct_extract_data_t> & check_set) {
        for (auto actor_obj : actor_list) {
            assert(actor_obj->actor_type == ACTOR_T::PROPERTIES);
            vector<uint64_t> local_check_set;

            // Analysis params
            Element_T inType = (Element_T)Tool::value_t2int(actor_obj->params.at(0));
            set<int> plist;
            for (int cnt = 1; cnt < actor_obj->params.size(); cnt++) {
                plist.emplace(Tool::value_t2int(actor_obj->params.at(cnt)));
            }

            // Compare check_set and parameters
            for (auto & val : check_set) {
                if (plist.find(get<1>(val)) != plist.end() && get<2>(val) == inType) {
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

    void get_properties_for_vertex(const QueryPlan & qplan, int tid, const vector<int> & key_list,
                                   vector<pair<history_t, vector<value_t>>>& data) {
        for (auto & pair : data) {
            vector<std::pair<string, string>> result;
            vector<value_t> newData;

            for (auto & value : pair.second) {
                vid_t v_id(Tool::value_t2int(value));

                if (key_list.empty()) {
                    // read all properties
                    vector<std::pair<label_t, value_t>> vp_kv_pair_list;
                    layout_storage_->GetVP(v_id, qplan.trxid, qplan.st, qplan.trx_type == TRX_READONLY, vp_kv_pair_list);

                    for (auto vp_kv_pair : vp_kv_pair_list) {
                        string keyStr;
                        layout_storage_->GetNameFromIndex(Index_T::V_PROPERTY, vp_kv_pair.first, keyStr);

                        result.emplace_back(keyStr, Tool::DebugString(vp_kv_pair.second));
                    }
                } else {
                    vector<vpid_t> vpid_list;
                    layout_storage_->GetVPidList(v_id, qplan.trxid, qplan.st, qplan.trx_type == TRX_READONLY, vpid_list);

                    for (auto pkey : key_list) {
                        if (find(vpid_list.begin(), vpid_list.end(), vpid_t(v_id, pkey)) == vpid_list.end()) {
                            continue;
                        }

                        vpid_t vp_id(v_id, pkey);
                        value_t val;

                        layout_storage_->GetVP(vp_id, qplan.trxid, qplan.st, qplan.trx_type == TRX_READONLY, val);

                        string keyStr;
                        layout_storage_->GetNameFromIndex(Index_T::V_PROPERTY, pkey, keyStr);

                        result.emplace_back(keyStr, Tool::DebugString(val));
                    }
                }
            }
            Tool::vec_pair2value_t(result, newData);
            pair.second.swap(newData);
        }
    }

    void get_properties_for_edge(const QueryPlan & qplan, int tid, const vector<int> & key_list,
                                 vector<pair<history_t, vector<value_t>>>& data) {
        for (auto & pair : data) {
            vector<std::pair<string, string>> result;
            vector<value_t> newData;

            for (auto & value : pair.second) {
                eid_t e_id;
                uint2eid_t(Tool::value_t2uint64_t(value), e_id);

                if (key_list.empty()) {
                    // read all properties
                    vector<std::pair<label_t, value_t>> ep_kv_pair_list;
                    layout_storage_->GetEP(e_id, qplan.trxid, qplan.st, qplan.trx_type == TRX_READONLY, ep_kv_pair_list);

                    for (auto ep_kv_pair : ep_kv_pair_list) {
                        string keyStr;
                        layout_storage_->GetNameFromIndex(Index_T::E_PROPERTY, ep_kv_pair.first, keyStr);

                        result.emplace_back(keyStr, Tool::DebugString(ep_kv_pair.second));
                    }
                } else {
                    vector<epid_t> epid_list;
                    layout_storage_->GetEPidList(e_id, qplan.trxid, qplan.st, qplan.trx_type == TRX_READONLY, epid_list);

                    for (auto pkey : key_list) {
                        if (find(epid_list.begin(), epid_list.end(), epid_t(e_id, pkey)) == epid_list.end()) {
                            continue;
                        }

                        epid_t ep_id(e_id, pkey);
                        value_t val;

                        layout_storage_->GetEP(ep_id, qplan.trxid, qplan.st, qplan.trx_type == TRX_READONLY, val);

                        string keyStr;
                        layout_storage_->GetNameFromIndex(Index_T::E_PROPERTY, pkey, keyStr);

                        result.emplace_back(keyStr, Tool::DebugString(val));
                    }
                }
            }
            Tool::vec_pair2value_t(result, newData);
            pair.second.swap(newData);
        }
    }
};

#endif  // ACTOR_PROPERTIES_ACTOR_HPP_
