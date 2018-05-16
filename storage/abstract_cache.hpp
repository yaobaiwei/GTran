#pragma once

#include <unordered_map>

class AbstractCache {
public:
  virtual ~AbstractCache() {}

  virtual int Get(int key) = 0;

  virtual void Insert() = 0;

  virtual void SetSize(int size) = 0;

private:
  virtual Evict() = 0;
};
