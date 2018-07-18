#ifndef TRAVERSAL_ACTOR_HPP_
#define TRAVERSAL_ACTOR_HPP_

#include <string>
#include <vector>

#include "actor/abstract_actor.hpp"
#include "core/message.hpp"
#include "core/abstract_mailbox.hpp"
#include "core/result_collector.hpp"
#include "base/node.hpp"
#include "base/type.hpp"
#include "storage/layout.hpp"
#include "storage/data_store.hpp"
#include "utils/tool.hpp"

// IN-OUT-BOTH
// TODO: Cache

using namespace std::placeholders;

class TraversalActor : public AbstractActor {
public:
    TraversalActor(int id, int num_thread, AbstractMailbox * mailbox, DataStore * datastore) : AbstractActor(id), num_thread_(num_thread), mailbox_(mailbox), datastore_(datastore), type_(ACTOR_T::TRAVERSAL) {
		fp = [&](value_t & v) { return get_node_id(v); } ;
	}

    // TraversalActorObject->Params;
    //  inType--outType--dir--lid
    //  dir: Direcntion:
    //           Vertex: IN/OUT/BOTH
    //                   INE/OUTE/BOTHE
    //             Edge: INV/OUTV/BOTHV
    //  lid: label_id (e.g. g.V().out("created"))
	void process(int tid, vector<Actor_Object> & actor_objs, Message & msg) {
        // Get Actor_Object
        Meta & m = msg.meta;
        Actor_Object actor_obj = actor_objs[m.step];

		// Get params
		Element_T inType = (Element_T) Tool::value_t2int(actor_obj.params.at(0));
		Element_T outType = (Element_T) Tool::value_t2int(actor_obj.params.at(1));
		Direction_T dir = (Direction_T) Tool::value_t2int(actor_obj.params.at(2));
		int lid = Tool::value_t2int(actor_obj.params.at(3));
		
		// Get Result
		if (inType == Element_T::VERTEX) {
			if (outType == Element_T::VERTEX) {
				GetNeighborOfVertex(tid, lid, dir, msg.data);
			} else if (outType == Element_T::EDGE) {
				GetEdgeOfVertex(tid, lid, dir, msg.data);
			} else {
				cout << "Wrong Out Element Type: " << outType << endl;
				return;
			}
		} else if (inType == Element_T::EDGE) {
			if (outType == Element_T::VERTEX) {
				GetVertexOfEdge(tid, lid, dir, msg.data);
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
        msg.CreateNextMsg(actor_objs, msg.data, num_thread_, msg_vec, &fp);
		
		// Send Message
		for (auto& msg : msg_vec) {
			mailbox_->Send(tid, msg);
		}
    }

	int get_node_id(value_t & v) {
		int type = v.type;
		if (type == 1) {
			vid_t v_id(Tool::value_t2int(v));
			return datastore_->GetMachineIdForVertex(v_id);
		} else if (type == 5) {
			uint64_t eid_value = Tool::value_t2uint64_t(v);
			uint64_t in_v = eid_value >> VID_BITS;
			uint64_t out_v = eid_value - (in_v << VID_BITS);

			eid_t e_id(in_v, out_v);
			return datastore_->GetMachineIdForEdge(e_id);
		} else {
			cout << "Wrong Type when getting node id" << type << endl;
			return -1;
		}
	}

private:
    // Number of Threads
    int num_thread_;

    // Actor type
    ACTOR_T type_;

    // Node
    Node node_;

    // Pointer of Result_Collector
    Result_Collector * rc_;

    // Pointer of mailbox
    AbstractMailbox * mailbox_;

    // Ensure only one thread ever runs the actor
    std::mutex thread_mutex_;

    // DataStore
    DataStore * datastore_;
		
	// Function pointer to assign dst 
	function<int(value_t &)> fp;


    //============Vertex===============
    // Get IN/OUT/BOTH of Vertex
    void GetNeighborOfVertex(int tid, int lid, Direction_T dir, vector<pair<history_t, vector<value_t>>> & data) {
        for (auto & pair : data) {
            vector<value_t> newData;

            for (auto & value : pair.second) {
				// Get the current vertex id and use it to get vertex instance
                vid_t cur_vtx_id(Tool::value_t2int(value));
                Vertex* vtx = datastore_->GetVertex(cur_vtx_id);

				// for each neighbor, create a new value_t and store into newData
				// IN & BOTH
				if (dir != Direction_T::OUT) {
					for (auto & in_nb : vtx->in_nbs) { // in_nb : vid_t
						// Get edge_id
						if (lid > 0) {
							eid_t e_id(in_nb.value(), cur_vtx_id.value());
							label_t label;
							datastore_->GetLabelForEdge(tid, e_id, label);

							if (label != lid) {
								continue;
							}
						}
						value_t new_value;
						Tool::str2value_t(to_string(in_nb.value()), new_value);
						newData.push_back(new_value);
					}
				}
				// OUT & BOTH
				if (dir != Direction_T::IN) {
					for (auto & out_nb : vtx->out_nbs) {
						if (lid > 0) {
							eid_t e_id(cur_vtx_id.value(), out_nb.value());
							label_t label;
							datastore_->GetLabelForEdge(tid, e_id, label);

							if (label != lid) {
								continue;
							}
						}
						value_t new_value;
						Tool::str2value_t(to_string(out_nb.value()), new_value);
						newData.push_back(new_value);
					}
				}
            }

			// Replace pair.second with new data 
			pair.second.swap(newData);
        }
	
	}

    // Get IN/OUT/BOTH-E of Vertex
    void GetEdgeOfVertex(int tid, int lid, Direction_T dir, vector<pair<history_t, vector<value_t>>> & data) {
        for (auto & pair : data) {
            vector<value_t> newData;

            for (auto & value : pair.second) {
				// Get the current vertex id and use it to get vertex instance
                vid_t cur_vtx_id(Tool::value_t2int(value));
                Vertex* vtx = datastore_->GetVertex(cur_vtx_id);

				if (dir != Direction_T::OUT) {
					for (auto & in_nb : vtx->in_nbs) { // in_nb : vid_t
						// Get edge_id
						eid_t e_id(in_nb.value(), cur_vtx_id.value());
						if (lid > 0) {
							label_t label;
							datastore_->GetLabelForEdge(tid, e_id, label);

							if (label != lid) {
								continue;
							}
						}
						value_t new_value;
						Tool::str2uint64_t(to_string(e_id.value()), new_value);
						newData.push_back(new_value);
					}
				}
				if (dir != Direction_T::IN) {
					for (auto & out_nb : vtx->out_nbs) {
						// Get edge_id
						eid_t e_id(cur_vtx_id.value(), out_nb.value());
						if (lid > 0) {
							label_t label;
							datastore_->GetLabelForEdge(tid, e_id, label);

							if (label != lid) {
								continue;
							}
						}
						value_t new_value;
						Tool::str2uint64_t(to_string(e_id.value()), new_value);
						newData.push_back(new_value);
					}
				}
            }

			// Replace pair.second with new data 
			pair.second.swap(newData);
        }
	}
    //=============Edge================
    void GetVertexOfEdge(int tid, int lid, Direction_T dir, vector<pair<history_t, vector<value_t>>> & data) {
        for (auto & pair : data) {
            vector<value_t> newData;

            for (auto & value : pair.second) {
            	uint64_t eid_value = Tool::value_t2uint64_t(value);
				uint64_t in_v = eid_value >> VID_BITS;
				uint64_t out_v = eid_value - (in_v << VID_BITS);

				if (dir == Direction_T::IN) {
					value_t new_value;
					Tool::str2value_t(to_string(in_v), new_value);
					newData.push_back(new_value);

				} else if (dir == Direction_T::OUT) {
					value_t new_value;
					Tool::str2value_t(to_string(out_v), new_value);
					newData.push_back(new_value);

				} else if (dir == Direction_T::BOTH) {
					value_t new_value_in;
					value_t new_value_out;
					Tool::str2value_t(to_string(in_v), new_value_in);
					Tool::str2value_t(to_string(out_v), new_value_out);
					newData.push_back(new_value_in);
					newData.push_back(new_value_out);
				} else {
					cout << "Wrong Direction Type" << endl;
					return;
				}
            }

			pair.second.swap(newData);
        }
	}

};
#endif /* TRAVERSAL_ACTOR_HPP_ */
