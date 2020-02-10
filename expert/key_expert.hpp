/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Aaron Li (cjli@cse.cuhk.edu.hk)
*/

#ifndef EXPERT_KEY_EXPERT_HPP_
#define EXPERT_KEY_EXPERT_HPP_

#include <string>
#include <utility>
#include <vector>

#include "core/message.hpp"
#include "core/abstract_mailbox.hpp"
#include "base/type.hpp"
#include "base/predicate.hpp"
#include "expert/abstract_expert.hpp"
#include "expert/expert_validation_object.hpp"
#include "utils/tool.hpp"

class KeyExpert : public AbstractExpert {
 public:
    KeyExpert(int id,
            int num_thread,
            AbstractMailbox * mailbox,
            CoreAffinity * core_affinity):
        AbstractExpert(id, core_affinity),
        num_thread_(num_thread),
        mailbox_(mailbox),
        type_(EXPERT_T::KEY) {
        config_ = Config::GetInstance();
    }

    // Key:
    //  Output all keys of properties of input
    // Parmas:
    //  inType
    void process(const QueryPlan & qplan, Message & msg) {
        int tid = TidMapper::GetInstance()->GetTid();

        // Get Expert_Object
        Meta & m = msg.meta;
        Expert_Object expert_obj = qplan.experts[m.step];

        // Get Params
        Element_T inType = (Element_T) Tool::value_t2int(expert_obj.params.at(0));

        if (qplan.trx_type != TRX_READONLY && config_->isolation_level == ISOLATION_LEVEL::SERIALIZABLE) {
            // Record Input Set
            for (auto & data_pair : msg.data) {
                v_obj.RecordInputSetValueT(qplan.trxid, expert_obj.index, inType, data_pair.second, m.step == 1 ? true : false);
            }
        }

        bool read_success = true;
        switch (inType) {
          case Element_T::VERTEX:
            read_success = VertexKeys(qplan, msg.data);
            break;
          case Element_T::EDGE:
            read_success = EdgeKeys(qplan, msg.data);
            break;
          default:
                cout << "Wrong in type"  << endl;
        }

        // Create Message
        vector<Message> msg_vec;
        if (read_success) {
            msg.CreateNextMsg(qplan.experts, msg.data, num_thread_, core_affinity_, msg_vec);
        } else {
            string abort_info = "Abort with [Processing][KeyExpert::process]";
            msg.CreateAbortMsg(qplan.experts, msg_vec, abort_info);
        }

        // Send Message
        for (auto& msg : msg_vec) {
            mailbox_->Send(tid, msg);
        }
    }

    bool valid(uint64_t TrxID, vector<Expert_Object*> & expert_list, const vector<rct_extract_data_t> & check_set) {
        for (auto & expert_obj : expert_list) {
            CHECK(expert_obj->expert_type == EXPERT_T::KEY);
            vector<uint64_t> local_check_set;

            // Analysis params
            Element_T inType = (Element_T) Tool::value_t2int(expert_obj->params.at(0));

            // Compare check_set and parameters
            for (auto & val : check_set) {
                if (get<2>(val) == inType) {
                    local_check_set.emplace_back(get<0>(val));
                }
            }

            if (local_check_set.size() != 0) {
                if (!v_obj.Validate(TrxID, expert_obj->index, local_check_set)) {
                    return false;
                }
            }
        }
        return true;
    }

    void clean_trx_data(uint64_t TrxID) { v_obj.DeleteInputSet(TrxID); }

 private:
    // Number of Threads
    int num_thread_;

    // Expert type
    EXPERT_T type_;

    // Pointer of mailbox
    AbstractMailbox * mailbox_;

    // Validation Store
    ExpertValidationObject v_obj;

    // Config
    Config * config_;

    bool VertexKeys(const QueryPlan & qplan, vector<pair<history_t, vector<value_t>>> & data) {
        for (auto & data_pair : data) {
            vector<value_t> newData;
            for (auto & elem : data_pair.second) {
                vid_t v_id(Tool::value_t2int(elem));

                vector<vpid_t> vp_list;
                READ_STAT read_status = data_storage_->GetVPidList(v_id, qplan.trxid, qplan.st, qplan.trx_type == TRX_READONLY, vp_list);
                if (read_status == READ_STAT::ABORT) {
                    return false;
                } else if (read_status == READ_STAT::NOTFOUND) {
                    continue;
                }

                for (auto & pkey : vp_list) {
                    string keyStr;
                    data_storage_->GetNameFromIndex(Index_T::V_PROPERTY, static_cast<label_t>(pkey.pid), keyStr);

                    value_t val;
                    Tool::str2str(keyStr, val);
                    newData.push_back(val);
                }
            }
            data_pair.second.swap(newData);
        }
        return true;
    }

    bool EdgeKeys(const QueryPlan & qplan, vector<pair<history_t, vector<value_t>>> & data) {
        for (auto & data_pair : data) {
            vector<value_t> newData;
            for (auto & elem : data_pair.second) {
                eid_t e_id;
                uint2eid_t(Tool::value_t2uint64_t(elem), e_id);

                vector<epid_t> ep_list;
                READ_STAT read_status = data_storage_->GetEPidList(e_id, qplan.trxid, qplan.st, qplan.trx_type == TRX_READONLY, ep_list);
                if (read_status == READ_STAT::ABORT) {
                    return false;
                } else if (read_status == READ_STAT::NOTFOUND) {
                    continue;
                }

                for (auto & pkey : ep_list) {
                    string keyStr;
                    data_storage_->GetNameFromIndex(Index_T::E_PROPERTY, static_cast<label_t>(pkey.pid), keyStr);

                    value_t val;
                    Tool::str2str(keyStr, val);
                    newData.push_back(val);
                }
            }
            data_pair.second.swap(newData);
        }
        return true;
    }
};

#endif  // EXPERT_KEY_EXPERT_HPP_
