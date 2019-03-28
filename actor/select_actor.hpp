/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Aaron Li (cjli@cse.cuhk.edu.hk)
*/

#ifndef ACTOR_SELECT_ACTOR_HPP_
#define ACTOR_SELECT_ACTOR_HPP_

#include <algorithm>
#include <string>
#include <utility>
#include <vector>

#include "actor/abstract_actor.hpp"
#include "core/message.hpp"
#include "core/abstract_mailbox.hpp"
#include "base/type.hpp"
#include "base/predicate.hpp"
#include "utils/tool.hpp"

class SelectActor : public AbstractActor {
 public:
    SelectActor(int id,
            int num_thread,
            AbstractMailbox * mailbox,
            CoreAffinity * core_affinity) :
        AbstractActor(id, core_affinity),
        num_thread_(num_thread),
        mailbox_(mailbox),
        type_(ACTOR_T::SELECT) {}

    void process(const QueryPlan & qplan, Message & msg) {
        int tid = TidMapper::GetInstance()->GetTid();

        // Get Actor_Object
        Meta & m = msg.meta;
        Actor_Object actor_obj = qplan.actors[m.step];

        assert(actor_obj.params.size() % 2 == 0);
        // Get Params
        vector<pair<int, string>> label_step_list;
        for (int i = 0; i < actor_obj.params.size(); i+=2) {
            label_step_list.emplace_back(
                    Tool::value_t2int(actor_obj.params.at(i)),
                    Tool::value_t2string(actor_obj.params.at(i + 1)));
        }

        // sort label_step_list for quick search
        sort(label_step_list.begin(), label_step_list.end(),
            [](const pair<int, string>& l, const pair<int, string>& r){ return l.first < r.first; });

        //  Grab history_t
        if (label_step_list.size() != 1) {
            GrabHistory(label_step_list, msg.data);
        } else {
            GrabHistory(label_step_list[0].first, msg.data);
        }

        // Create Message
        vector<Message> msg_vec;
        msg.CreateNextMsg(qplan.actors, msg.data, num_thread_, core_affinity_, msg_vec);

        // Send Message
        for (auto& msg : msg_vec) {
            mailbox_->Send(tid, msg);
        }
     }

 private:
    // Number of Threads
    int num_thread_;

    // Actor type
    ACTOR_T type_;

    // Pointer of mailbox
    AbstractMailbox * mailbox_;

    void GrabHistory(vector<pair<int, string>> label_step_list, vector<pair<history_t, vector<value_t>>> & data) {
        vector<value_t> result;

        for (auto & data_pair : data) {
            int value_size = data_pair.second.size();
            string res = "[";
            bool isResultEmpty = true;

            auto l_itr = label_step_list.begin();

            if (!data_pair.first.empty()) {
                vector<pair<int, value_t>>::iterator p_itr = data_pair.first.begin();

                // once there is one list ends, end search
                do {
                    if (l_itr->first == p_itr->first) {
                        res += l_itr->second + ":" + Tool::DebugString(p_itr->second) + ", ";
                        isResultEmpty = false;

                        l_itr++;
                        p_itr++;
                    } else if (l_itr->first < p_itr->first) {
                        l_itr++;
                    } else if (l_itr->first > p_itr->first) {
                        p_itr++;
                    }
                } while (l_itr != label_step_list.end() && p_itr != data_pair.first.end());
            }

            if (!isResultEmpty) {
                res.pop_back();
                res.pop_back();
            }
            res += "]";

            if (!data_pair.first.empty() && !isResultEmpty) {
                for (int i = 0; i < data_pair.second.size(); i++) {
                    value_t val;
                    Tool::str2str(res, val);
                    result.push_back(val);
                }
            }

            data_pair.second.swap(result);
            result.clear();
        }
    }

    void GrabHistory(int label_step, vector<pair<history_t, vector<value_t>>> & data) {
        for (auto & data_pair : data) {
            vector<value_t> result;
            if (!data_pair.first.empty()) {
                vector<pair<int, value_t>>::iterator p_itr = data_pair.first.begin();
                do {
                    if (label_step == (*p_itr).first) {
                        for (int i = 0; i < data_pair.second.size(); i++) {
                            result.push_back((*p_itr).second);
                        }
                        break;
                    }
                    p_itr++;
                } while (p_itr != data_pair.first.end());
            }
            data_pair.second.swap(result);
        }
    }
};

#endif  // ACTOR_SELECT_ACTOR_HPP_
