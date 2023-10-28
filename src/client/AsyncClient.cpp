/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "nebula/client/AsyncClient.h"

#include <folly/SocketAddress.h>
#include <folly/executors/IOThreadPoolExecutor.h>
#include <folly/io/async/AsyncSSLSocket.h>
#include <folly/io/async/AsyncSocket.h>
#include <folly/io/async/ScopedEventBaseThread.h>
#include <thrift/lib/cpp2/async/HeaderClientChannel.h>

#include <memory>
#include <stdexcept>

#include "interface/gen-cpp2/GraphServiceAsyncClient.h"

namespace nebula {

AsyncClient::AsyncClient(folly::IOThreadPoolExecutor* ioExecutor): client_{nullptr}, ioExecutor_{ioExecutor} {}

AsyncClient::~AsyncClient() {
  close();
}

AsyncClient &AsyncClient::operator=(AsyncClient &&c) {
  close();
  client_ = c.client_;
  c.client_ = nullptr;
  ioExecutor_ = c.ioExecutor_;
  c.ioExecutor_ = nullptr;
  return *this;
}

bool AsyncClient::open(const std::string &address,
                       int32_t port,
                       uint32_t conn_timeout,
                       uint32_t channel_timeout) {
  if (address.empty()) {
    return false;
  }
  bool complete{false};
  std::shared_ptr<folly::AsyncSocket> socket;
  folly::SocketAddress socketAddr;
  try {
    socketAddr = folly::SocketAddress(address, port, true);
  } catch (const std::exception &e) {
    DLOG(ERROR) << "Invalid address: " << address << ":" << port << ": " << e.what();
    return false;
  }
  auto *evb = ioExecutor_->getEventBase();
  evb->runImmediatelyOrRunInEventBaseThreadAndWait(
      [this, evb, &complete, &socket, conn_timeout, &socketAddr]() {
        try {
          socket = folly::AsyncSocket::newSocket(evb, std::move(socketAddr), conn_timeout);
          complete = true;
          // 这里不需要再手动调用socket.connect，是因为 1. newSocket本身会调用connect。2. 在non-blocking mode下，connect也是
          // 异步的，AsyncSocket有统一处理的处理逻辑handleConnect()。后面调用socket.send时，如果发现还没connect成功，
          // folly会自己处理
        } catch (const std::exception &e) {
          DLOG(ERROR) << "Connect failed: " << e.what();
          complete = false;
        }
      });
  if (!complete) {
    return false;
  }
  if (!socket->good()) {
    return false;
  }
  // TODO workaround for issue #72
  // socket->setErrMessageCB(&NebulaConnectionErrMessageCallback::instance());
  auto channel = apache::thrift::HeaderClientChannel::newChannel(socket);
  channel->setTimeout(channel_timeout);
  client_ = new graph::cpp2::GraphServiceAsyncClient(std::move(channel));
  return true;
}

void AsyncClient::authenticate(const std::string &user,
                               const std::string &password,
                               AuthCallback cb) {
  if (client_ == nullptr) {
    cb(AuthResponse{
        ErrorCode::E_DISCONNECTED, 0, std::make_unique<std::string>("Not open connection.")});
    return;
  }
  client_->future_authenticate(user, password)
      .thenTry([cb = std::move(cb)](folly::Try<AuthResponse> &&respTry) {
        using TTransportException = apache::thrift::transport::TTransportException;
        if (respTry.hasException()) {
          auto &tryException = respTry.exception();
          if (auto ex = tryException.get_exception<TTransportException>(); ex != nullptr) {
            auto errType = ex->getType();
            std::string errMsg = ex->what();
            if (errType == TTransportException::END_OF_FILE ||
                (errType == TTransportException::INTERNAL_ERROR &&
                 errMsg.find("Connection reset by peer") != std::string::npos) ||
                (errType == TTransportException::UNKNOWN &&
                 errMsg.find("Channel is !good()") != std::string::npos)) {
              cb(AuthResponse{
                  ErrorCode::E_FAIL_TO_CONNECT, nullptr, std::make_unique<std::string>(errMsg)});
            } else if (errType == TTransportException::TIMED_OUT) {
              cb(AuthResponse{
                  ErrorCode::E_SESSION_TIMEOUT, nullptr, std::make_unique<std::string>(errMsg)});
            } else {
              cb(AuthResponse{
                  ErrorCode::E_RPC_FAILURE, nullptr, std::make_unique<std::string>(errMsg)});
            }
          } else {
            std::string errMsg =
                tryException.class_name().toStdString() + " : " + tryException.what().toStdString();
            cb(AuthResponse{
                ErrorCode::E_RPC_FAILURE, nullptr, std::make_unique<std::string>(errMsg)});
          }
        } else {
          cb(std::move(respTry.value()));
        }
      });
}

void AsyncClient::execute(int64_t sessionId, const std::string &stmt, ExecuteCallback cb) {
  if (client_ == nullptr) {
    cb(ExecutionResponse{ErrorCode::E_DISCONNECTED,
                         0,
                         nullptr,
                         nullptr,
                         std::make_unique<std::string>("Not open connection.")});
    return;
  }
  client_->future_execute(sessionId, stmt)
      .thenTry([cb = std::move(cb)](folly::Try<ExecutionResponse> &&respTry) {
        using TTransportException = apache::thrift::transport::TTransportException;
        if (respTry.hasException()) {
          auto &tryException = respTry.exception();
          if (auto ex = tryException.get_exception<TTransportException>(); ex != nullptr) {
            auto errType = ex->getType();
            std::string errMsg = ex->what();
            if (errType == TTransportException::END_OF_FILE ||
                (errType == TTransportException::INTERNAL_ERROR &&
                 errMsg.find("Connection reset by peer") != std::string::npos) ||
                (errType == TTransportException::UNKNOWN &&
                 errMsg.find("Channel is !good()") != std::string::npos)) {
              cb(ExecutionResponse{ErrorCode::E_FAIL_TO_CONNECT,
                                   0,
                                   nullptr,
                                   nullptr,
                                   std::make_unique<std::string>(errMsg)});
            } else if (errType == TTransportException::TIMED_OUT) {
              cb(ExecutionResponse{ErrorCode::E_SESSION_TIMEOUT,
                                   0,
                                   nullptr,
                                   nullptr,
                                   std::make_unique<std::string>(errMsg)});
            } else {
              cb(ExecutionResponse{ErrorCode::E_RPC_FAILURE,
                                   0,
                                   nullptr,
                                   nullptr,
                                   std::make_unique<std::string>(errMsg)});
            }
          } else {
            std::string errMsg =
                tryException.class_name().toStdString() + " : " + tryException.what().toStdString();
            cb(ExecutionResponse{ErrorCode::E_RPC_FAILURE,
                                 0,
                                 nullptr,
                                 nullptr,
                                 std::make_unique<std::string>(errMsg)});
          }
        } else {
          cb(std::move(respTry.value()));
        }
      });
}

void AsyncClient::close() {
  if (client_ != nullptr) {
    auto* evb = client_->getChannelShared()->getEventBase();
    evb->runImmediatelyOrRunInEventBaseThreadAndWait([this]() { delete client_; });
    client_ = nullptr;
  }
}


AsyncClientFactory::AsyncClientFactory(const size_t num_netio_threads) {
  size_t num_threads = (num_netio_threads == 0 ? std::thread::hardware_concurrency() : num_netio_threads);
  ioExecutor_ = new folly::IOThreadPoolExecutor(num_threads);
}

AsyncClientFactory::~AsyncClientFactory() {
  delete ioExecutor_;
}

AsyncClient&& AsyncClientFactory::CreateInstance() {
  return std::move(AsyncClient(ioExecutor_));
}



}  // namespace nebula
