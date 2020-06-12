// Copyright 2019 BigGraph Team @ Husky Data Lab, CUHK
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef EXPERT_TRAVERSAL_EXPERT_HPP_
#define EXPERT_TRAVERSAL_EXPERT_HPP_

#include <string>
#include <utility>
#include <vector>

#include "base/node.hpp"
#include "base/type.hpp"
#include "core/message.hpp"
#include "core/abstract_mailbox.hpp"
#include "core/result_collector.hpp"
#include "expert/abstract_expert.hpp"
#include "expert/expert_validation_object.hpp"
#include "utils/tool.hpp"

// IN-OUT-BOTH

using namespace std::placeholders;

class TraversalExpert : public AbstractExpert {
 public:
    TraversalExpert(int id,
            int num_thread,
            AbstractMailbox * mailbox,
            CoreAffinity * core_affinity) :
        AbstractExpert(id, core_affinity),
        num_thread_(num_thread),
        mailbox_(mailbox),
        type_(EXPERT_T::TRAVERSAL) {
        config_ = Config::GetInstance();
    }

    // TraversalExpertObject->Params;
    //  inType--outType--dir--lid
    //  dir: Direcntion:
    //           Vertex: IN/OUT/BOTH
    //                   INE/OUTE/BOTHE
    //             Edge: INV/OUTV/BOTHV
    //  lid: label_id (e.g. g.V().out("created"))
    void process(const QueryPlan & qplan, Message & msg) {
        int tid = TidPoolManager::GetInstance()->GetTid(TID_TYPE::RDMA);

        // Get Expert_Object
        Meta & m = msg.meta;
        Expert_Object expert_obj = qplan.experts[m.step];

        // Get params
        Element_T inType = (Element_T) Tool::value_t2int(expert_obj.params.at(0));
        Element_T outType = (Element_T) Tool::value_t2int(expert_obj.params.at(1));
        Direction_T dir = (Direction_T) Tool::value_t2int(expert_obj.params.at(2));
        int lid = Tool::value_t2int(expert_obj.params.at(3));
        // Parser treat -1 as empty input, while DataStorage is 0 since it use label_t (uint16_t)
        lid = (lid == -1) ? 0 : lid;

        if (qplan.trx_type != TRX_READONLY && config_->isolation_level == ISOLATION_LEVEL::SERIALIZABLE) {
            // Record Input Set
            for (auto & data_pair : msg.data) {
                v_obj.RecordInputSetValueT(qplan.trxid, expert_obj.index, inType, data_pair.second, m.step == 1 ? true : false);
                PushToRWRecord(qplan.trxid, data_pair.second.size(), true);
            }
        }

        // Get Result
        bool read_success = true;
        if (inType == Element_T::VERTEX) {
            if (outType == Element_T::VERTEX) {
                read_success = GetNeighborOfVertex(qplan, lid, dir, msg.data);
            } else if (outType == Element_T::EDGE) {
                read_success = GetEdgeOfVertex(qplan, lid, dir, msg.data);
            } else {
                cout << "Wrong Out Element Type: " << outType << endl;
                return;
            }
        } else if (inType == Element_T::EDGE) {
            if (outType == Element_T::VERTEX) {
                GetVertexOfEdge(lid, dir, msg.data);
            } else {
                cout << "Wrong Out Element Type: " << outType << endl;
                return;
            }
        } else {
            cout << "Wrong In Element Type: " << inType << endl;
            return;
        }

        // Create Message
        vector<Message> msg_vec;
        if (read_success) {
            msg.CreateNextMsg(qplan.experts, msg.data, num_thread_, core_affinity_, msg_vec);
        } else {
            string abort_info = "Abort with [Processing][TraversalExpert::process]";
            msg.CreateAbortMsg(qplan.experts, msg_vec, abort_info);
        }

        // Send Message
        for (auto& msg : msg_vec) {
            mailbox_->Send(tid, msg);
        }
    }

    bool valid(uint64_t TrxID, vector<Expert_Object*> & expert_list, const vector<rct_extract_data_t> & check_set) {
        for (auto & expert_obj : expert_list) {
            CHECK(expert_obj->expert_type == EXPERT_T::TRAVERSAL);
            vector<uint64_t> local_check_set;

            // Analysis params
            Element_T inType = (Element_T) Tool::value_t2int(expert_obj->params.at(0));

            // Compare check_set and parameters
            for (auto & val : check_set) {
                // pid = 0 -> label which means there must be an edge was operated
                if (get<1>(val) == 0 && get<2>(val) == inType) {
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
    Config* config_;

    // Validation Store
    ExpertValidationObject v_obj;

    // ============Vertex===============
    // Get IN/OUT/BOTH of Vertex
    bool GetNeighborOfVertex(const QueryPlan & qplan, int lid, Direction_T dir, vector<pair<history_t, vector<value_t>>> & data) {
        for (auto& pair : data) {
            vector<value_t> newData;

            for (auto & value : pair.second) {
                // Get the current vertex id and use it to get vertex instance
                vid_t cur_vtx_id(Tool::value_t2int(value));
                vector<vid_t> v_nbs;
                READ_STAT read_status = data_storage_->
                                        GetConnectedVertexList(cur_vtx_id, lid, dir, qplan.trxid, qplan.st, qplan.trx_type == TRX_READONLY, v_nbs);
                if (read_status == READ_STAT::ABORT) {
                    return false;
                } else if (read_status == READ_STAT::NOTFOUND) {
                    continue;
                }

                for (auto & neighbor : v_nbs) {
                    value_t new_value;
                    Tool::str2int(to_string(neighbor.value()), new_value);
                    newData.push_back(new_value);
                }
            }

            // Replace pair.second with new data
            pair.second.swap(newData);
        }
        return true;
    }

    // Get IN/OUT/BOTH-E of Vertex
    bool GetEdgeOfVertex(const QueryPlan & qplan, int lid, Direction_T dir, vector<pair<history_t, vector<value_t>>> & data) {
        for (auto& pair : data) {
            vector<value_t> newData;

            for (auto & value : pair.second) {
                // Get the current vertex id and use it to get vertex instance
                vid_t cur_vtx_id(Tool::value_t2int(value));
                vector<eid_t> e_nbs;
                READ_STAT read_status = data_storage_->
                                        GetConnectedEdgeList(cur_vtx_id, lid, dir, qplan.trxid, qplan.st, qplan.trx_type == TRX_READONLY, e_nbs);
                if (read_status == READ_STAT::ABORT) {
                    return false;
                } else if (read_status == READ_STAT::NOTFOUND) {
                    continue;
                }

                for (auto & neighbor : e_nbs) {
                    value_t new_value;
                    Tool::str2uint64_t(to_string(neighbor.value()), new_value);
                    newData.push_back(new_value);
                }
            }

            // Replace pair.second with new data
            pair.second.swap(newData);
        }
        return true;
    }

    // =============Edge================
    void GetVertexOfEdge(int lid, Direction_T dir, vector<pair<history_t, vector<value_t>>> & data) {
        for (auto & pair : data) {
            vector<value_t> newData;

            for (auto & value : pair.second) {
                uint64_t eid_value = Tool::value_t2uint64_t(value);
                uint64_t dst_v = eid_value >> VID_BITS;
                uint64_t src_v = eid_value - (dst_v << VID_BITS);

                if (dir == Direction_T::IN) {
                    value_t new_value;
                    Tool::str2int(to_string(dst_v), new_value);
                    newData.push_back(new_value);
                } else if (dir == Direction_T::OUT) {
                    value_t new_value;
                    Tool::str2int(to_string(src_v), new_value);
                    newData.push_back(new_value);
                } else if (dir == Direction_T::BOTH) {
                    value_t new_value_in;
                    value_t new_value_out;
                    Tool::str2int(to_string(dst_v), new_value_in);
                    Tool::str2int(to_string(src_v), new_value_out);
                    newData.push_back(new_value_in);
                    newData.push_back(new_value_out);
                } else {
                    cout << "Wrong Direction Type" << endl;
                    return;
                }
            }

            // Replace pair.second with new data
            pair.second.swap(newData);
        }
    }
};
#endif  // EXPERT_TRAVERSAL_EXPERT_HPP_
