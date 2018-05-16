/*
 * mailbox.hpp
 *
 *  Created on: May 9, 2018
 *      Author: Hongzhi Chen
 */

#pragma once

#include "base/abstract_id_mapper.hpp"
#include "base/abstract_mailbox.hpp"
#include "core/thread_safe_queue.hpp"


#include <atomic>
#include <map>
#include <thread>
#include <unordered_map>
#include <vector>

#include <zmq.h>

class Mailbox : public AbstractMailbox {
public:
  Mailbox(const Node &node, const std::vector<Node> &nodes,
          AbstractIdMapper *id_mapper);
  virtual void RegisterQueue(uint32_t queue_id,
                             ThreadSafeQueue<Message> *const queue) override;
  virtual void DeregisterQueue(uint32_t queue_id) override;
  virtual int Send(const Message &msg) override;
  int Recv(Message *msg);
  void Start();
  void Stop();
  size_t GetQueueMapSize() const;
  void Barrier();

  // For testing only
  void ConnectAndBind();
  void StartReceiving();
  void StopReceiving();
  void CloseSockets();

private:
  void Connect(const Node &node);
  void Bind(const Node &node);

  void Receiving();

  std::map<uint32_t, ThreadSafeQueue<Message> *const> queue_map_;
  // Not owned
  AbstractIdMapper *id_mapper_;

  std::thread receiver_thread_;

  // node
  Node node_;
  std::vector<Node> nodes_;

  // socket
  void *context_ = nullptr;
  std::unordered_map<uint32_t, void *> senders_;
  void *receiver_ = nullptr;
  std::mutex mu_;

  // barrier
  std::mutex barrier_mu_;
  std::condition_variable barrier_cond_;
  int barrier_count_ = 0;
};
