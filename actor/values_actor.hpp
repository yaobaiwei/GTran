/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Aaron Li (cjli@cse.cuhk.edu.hk)
*/

#ifndef ACTOR_VALUES_ACTOR_HPP_
#define ACTOR_VALUES_ACTOR_HPP_

#include <string>
#include <utility>
#include <vector>

#include "actor/abstract_actor.hpp"
#include "actor/actor_validation_object.hpp"
#include "core/message.hpp"
#include "core/abstract_mailbox.hpp"
#include "base/type.hpp"
#include "utils/tool.hpp"

class ValuesActor : public AbstractActor {
 public:
    ValuesActor(int id,
            int machine_id,
            int num_thread,
            AbstractMailbox * mailbox,
            CoreAffinity * core_affinity) :
        AbstractActor(id, core_affinity),
        machine_id_(machine_id),
        num_thread_(num_thread),
        mailbox_(mailbox),
        type_(ACTOR_T::VALUES) {
        config_ = Config::GetInstance();
    }

    // inType, [key]+
    void process(const QueryPlan & qplan, Message & msg) {
        int tid = TidMapper::GetInstance()->GetTid();

        Meta & m = msg.meta;
        Actor_Object actor_obj = qplan.actors[m.step];

        Element_T inType = (Element_T)Tool::value_t2int(actor_obj.params.at(0));
        vector<label_t> key_list;
        for (int cnt = 1; cnt < actor_obj.params.size(); cnt++) {
            key_list.emplace_back(static_cast<label_t>(Tool::value_t2int(actor_obj.params.at(cnt))));
        }

        if (qplan.trx_type != TRX_READONLY && config_->isolation_level == ISOLATION_LEVEL::SERIALIZABLE) {
            // Record Input Set
            for (auto & data_pair : msg.data) {
                v_obj.RecordInputSetValueT(qplan.trxid, actor_obj.index, inType, data_pair.second, m.step == 1 ? true : false);
            }
        }

        bool read_success = true;
        switch (inType) {
          case Element_T::VERTEX:
            read_success = get_properties_for_vertex(qplan, key_list, msg.data);
            break;
          case Element_T::EDGE:
            read_success = get_properties_for_edge(qplan, key_list, msg.data);
            break;
          default:
                cout << "Wrong in type" << endl;
        }

        vector<Message> msg_vec;
        if (read_success) {
            msg.CreateNextMsg(qplan.actors, msg.data, num_thread_, core_affinity_, msg_vec);
        } else {
            string abort_info = "Abort with [Processing][ValuesActor::process]";
            msg.CreateAbortMsg(qplan.actors, msg_vec, abort_info);
        }

        // Send Message
        for (auto& msg : msg_vec) {
            mailbox_->Send(tid, msg);
        }
    }

    bool valid(uint64_t TrxID, vector<Actor_Object*> & actor_list, const vector<rct_extract_data_t> & check_set) {
        for (auto & actor_obj : actor_list) {
            assert(actor_obj->actor_type == ACTOR_T::VALUES);
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
    Config * config_;

    // Validation Store
    ActorValidationObject v_obj;

    bool get_properties_for_vertex(const QueryPlan & qplan, const vector<label_t> & key_list, vector<pair<history_t, vector<value_t>>>& data) {
        for (auto & pair : data) {
            vector<value_t> newData;

            for (auto & value : pair.second) {
                vid_t v_id(Tool::value_t2int(value));
                vector<std::pair<label_t, value_t>> vp_kv_pair_list;

                READ_STAT read_status;
                if (key_list.empty()) {
                    read_status = data_storage_->GetAllVP(v_id, qplan.trxid, qplan.st, qplan.trx_type == TRX_READONLY, vp_kv_pair_list);
                } else {
                    read_status = data_storage_->GetVPByPKeyList(v_id, key_list, qplan.trxid, qplan.st, qplan.trx_type == TRX_READONLY, vp_kv_pair_list);
                }

                if (read_status == READ_STAT::ABORT) {
                    return false;
                } else if (read_status == READ_STAT::NOTFOUND) {
                    continue;
                }

                for (auto vp_kv_pair : vp_kv_pair_list) {
                    newData.emplace_back(vp_kv_pair.second);
                }
            }
            pair.second.swap(newData);
        }
        return true;
    }

    bool get_properties_for_edge(const QueryPlan & qplan, const vector<label_t> & key_list, vector<pair<history_t, vector<value_t>>>& data) {
        for (auto & pair : data) {
            vector<value_t> newData;

            for (auto & value : pair.second) {
                eid_t e_id;
                uint2eid_t(Tool::value_t2uint64_t(value), e_id);
                vector<std::pair<label_t, value_t>> ep_kv_pair_list;

                READ_STAT read_status;
                if (key_list.empty()) {
                    read_status = data_storage_->GetAllEP(e_id, qplan.trxid, qplan.st, qplan.trx_type == TRX_READONLY, ep_kv_pair_list);
                } else {
                    read_status = data_storage_->GetEPByPKeyList(e_id, key_list, qplan.trxid, qplan.st, qplan.trx_type == TRX_READONLY, ep_kv_pair_list);
                }

                if (read_status == READ_STAT::ABORT) {
                    return false;
                } else if (read_status == READ_STAT::NOTFOUND) {
                    continue;
                }

                for (auto ep_kv_pair : ep_kv_pair_list) {
                    newData.emplace_back(ep_kv_pair.second);
                }
            }
            pair.second.swap(newData);
        }
        return true;
    }
};

#endif  // ACTOR_VALUES_ACTOR_HPP_
