#pragma once

#include "core/message.hpp"

#include <string>

class AbstractMailbox {
public:
  virtual ~AbstractMailbox() {}

  virtual int Send(int tid, const Message & msg) = 0;
  virtual bool TryRecv(int tid, Message & msg) = 0;
  virtual void Recv(int tid, Message & msg) = 0;
};
