/*
 * abstract_actor.hpp
 *
 *  Created on: May 26, 2018
 *      Author: Hongzhi Chen
 */
//
#ifndef ABSTRACT_ACTOR_HPP_
#define ABSTRACT_ACTOR_HPP_

#include <string>
#include <vector>

#include "core/message.hpp"

class AbstractActor {
public:
  virtual ~AbstractActor() {}

  virtual const int GetActorId() = 0;

  template <class T>
  virtual void process(int t_id, Message<T> & msg) = 0;

private:
};

#endif /* ABSTRACT_ACTOR_HPP_ */
