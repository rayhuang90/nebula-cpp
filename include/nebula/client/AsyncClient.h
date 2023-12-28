/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#pragma once

#include <functional>
#include <memory>
#include <string>

#include "common/datatypes/Value.h"
#include "common/graph/Response.h"

namespace folly {
class IOThreadPoolExecutor;
}  // namespace folly

namespace nebula {

// Wrap the thrift client.
namespace graph {
namespace cpp2 {
class GraphServiceAsyncClient;
}
}  // namespace graph

class AsyncClient {
 public:
  using AuthCallback = std::function<void(AuthResponse &&)>;
  using ExecuteCallback = std::function<void(ExecutionResponse &&)>;
  using ExecuteJsonCallback = std::function<void(std::string &&)>;

  AsyncClient(folly::IOThreadPoolExecutor* ioExecutor);
  // disable copy
  AsyncClient(const AsyncClient &) = delete;
  AsyncClient &operator=(const AsyncClient &c) = delete;

  AsyncClient(AsyncClient &&c) noexcept {
    client_ = c.client_;
    c.client_ = nullptr;
    ioExecutor_ = c.ioExecutor_;
    c.ioExecutor_ = nullptr;
  }

  AsyncClient &operator=(AsyncClient &&c);

  ~AsyncClient();

  bool open(const std::string &address, int32_t port, uint32_t conn_timeout, uint32_t channel_timeout);

  void authenticate(const std::string &user, const std::string &password, AuthCallback cb);

  void execute(int64_t sessionId, const std::string &stmt, ExecuteCallback cb);

  void close();

 private:
  graph::cpp2::GraphServiceAsyncClient *client_{nullptr};
  folly::IOThreadPoolExecutor* ioExecutor_{nullptr};
};

class AsyncClientFactory {
 public:
  AsyncClientFactory(const size_t num_netio_threads = 0);
  // disable copy
  AsyncClientFactory(const AsyncClientFactory &) = delete;
  AsyncClientFactory &operator=(const AsyncClientFactory &c) = delete;

  AsyncClientFactory(AsyncClientFactory &&c) noexcept {
    ioExecutor_ = c.ioExecutor_;
    c.ioExecutor_ = nullptr;
  }

  AsyncClientFactory &operator=(AsyncClientFactory &&c) = delete;

  ~AsyncClientFactory();

  AsyncClient&& CreateInstance();

 private:
  folly::IOThreadPoolExecutor* ioExecutor_{nullptr};
};

}  // namespace nebula
