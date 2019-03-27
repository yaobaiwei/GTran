/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Aaron Li (cjli@cse.cuhk.edu.hk)
*/

#ifndef ACTOR_IS_ACTOR_HPP_
#define ACTOR_IS_ACTOR_HPP_

#include <string>
#include <utility>
#include <vector>

#include "actor/abstract_actor.hpp"
#include "core/message.hpp"
#include "core/abstract_mailbox.hpp"
#include "base/type.hpp"
#include "base/predicate.hpp"
#include "storage/layout.hpp"
#include "utils/tool.hpp"

class IsActor : public AbstractActor {
 public:
    IsActor(int id,
            int num_thread,
            AbstractMailbox * mailbox,
            CoreAffinity* core_affinity) :
        AbstractActor(id, core_affinity),
        num_thread_(num_thread),
        mailbox_(mailbox),
        type_(ACTOR_T::IS) {}

    // [pred_T , pred_params]...
    void process(const QueryPlan & qplan, Message & msg) {
        int tid = TidMapper::GetInstance()->GetTid();

        // Get Actor_Object
        Meta & m = msg.meta;
        Actor_Object actor_obj = qplan.actors[m.step];

        // Get Params
        vector<PredicateValue> pred_chain;

        assert(actor_obj.params.size() > 0 && (actor_obj.params.size() % 2) == 0);
        int numParamsGroup = actor_obj.params.size() / 2;

        for (int i = 0; i < numParamsGroup; i++) {
            int pos = i * 2;
            // Get predicate params
            Predicate_T pred_type = (Predicate_T) Tool::value_t2int(actor_obj.params.at(pos));
            vector<value_t> pred_params;
            Tool::value_t2vec(actor_obj.params.at(pos + 1), pred_params);

            pred_chain.emplace_back(pred_type, pred_params);
        }

        // Evaluate
        EvaluateData(msg.data, pred_chain);

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

    void EvaluateData(vector<pair<history_t, vector<value_t>>> & data, vector<PredicateValue> & pred_chain) {
        auto checkFunction = [&](value_t & value) {
            int counter = pred_chain.size();
            for (auto & pred : pred_chain) {
                if (Evaluate(pred, &value)) {
                    counter--;
                }
            }

            // Not match all pred
            if (counter != 0) {
                return true;
            }
            return false;
        };

        for (auto & data_pair : data) {
            data_pair.second.erase(
                    remove_if(data_pair.second.begin(), data_pair.second.end(), checkFunction), data_pair.second.end());
        }
    }
};

#endif  // ACTOR_IS_ACTOR_HPP_
