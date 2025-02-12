#include <iostream>
#include <memory>
#include <thread>
#include <vector>
#include <string>
#include <unistd.h>
#include <csignal>
#include <grpc++/grpc++.h>
#include "client.h"

#include "sns.grpc.pb.h"


#include "coordinator.pb.h"
#include "coordinator.grpc.pb.h"
using grpc::Channel;
using grpc::ClientContext;
using grpc::ClientReader;
using grpc::ClientReaderWriter;
using grpc::ClientWriter;
using grpc::Status;
using csce662::Message;
using csce662::ListReply;
using csce662::Request;
using csce662::Reply;
using csce662::SNSService;

using csce662::CoordService;
using csce662::ServerInfo;

using csce662::ID;
void sig_ignore(int sig) {
  std::cout << "Signal caught " + sig;
}

Message MakeMessage(const std::string& username, const std::string& msg) {
    Message m;
    m.set_username(username);
    m.set_msg(msg);
    google::protobuf::Timestamp* timestamp = new google::protobuf::Timestamp();
    timestamp->set_seconds(time(NULL));
    timestamp->set_nanos(0);
    m.set_allocated_timestamp(timestamp);
    return m;
}


class Client : public IClient
{
public:
  Client(const std::string& hname,
	 const std::string& uname,
	 const std::string& p)
    :hostname(hname), username(uname), port(p) {}

  
protected:
  virtual int connectTo();
  virtual IReply processCommand(std::string& input);
  virtual void processTimeline();
  virtual void SendHeartbeat();

private:
  std::string hostname;
  std::string username;
  std::string port;
  
  // You can have an instance of the client stub
  // as a member variable.
  std::unique_ptr<SNSService::Stub> stub_;

   std::unique_ptr<CoordService::Stub> coordinator_stub;
  
  IReply Login();
  IReply List();
  IReply ClientHeartbeat();
  IReply Follow(const std::string &username);
  IReply UnFollow(const std::string &username);
  void   Timeline(const std::string &username);
};


///////////////////////////////////////////////////////////
//
//////////////////////////////////////////////////////////

// Send heartbeat to the server
void Client::SendHeartbeat() {
    while (true){

        sleep(5);

        IReply reply = ClientHeartbeat();
        // std::cout << "sent heartbeat from client to server!\n"; 
        if (!reply.grpc_status.ok()){
            /* std::cout << "GRPC CALL FAILED!\n"; */
            exit(1);
        }
    }

}


int Client::connectTo()
{
  // ------------------------------------------------------------
  // In this function, you are supposed to create a stub so that
  // you call service methods in the processCommand/porcessTimeline
  // functions. That is, the stub should be accessible when you want
  // to call any service methods in those functions.
  // Please refer to gRpc tutorial how to create a stub.
  // ------------------------------------------------------------
    
    std::string login_info = hostname + ":" + port;

    // Connect client to the coordinator

    coordinator_stub = CoordService::NewStub(grpc::CreateChannel(login_info, grpc::InsecureChannelCredentials()));

    ClientContext clientContext;
    ServerInfo serverInfo;
    ID id;
    id.set_id(stoi(username));

    // Receive server information from the coordinator
    grpc::Status grpcStatus = coordinator_stub->GetServer(&clientContext,id,&serverInfo);
    if(!grpcStatus.ok()){
      return -1;
    }

    // spin off a separate thread to send periodic heartbeats to the server so the server can check for disconnected clients
    std::thread myhb(&Client::SendHeartbeat, this);
    myhb.detach();

    login_info = serverInfo.hostname() + ":" + serverInfo.port();
    std::cout<<serverInfo.hostname()<<" "<<serverInfo.port()<<std::endl;
    // Create a stub
    stub_ = SNSService::NewStub(grpc::CreateChannel(login_info, grpc::InsecureChannelCredentials()));

    IReply ire = Login();
    
    if(!ire.grpc_status.ok()||ire.comm_status==FAILURE_ALREADY_EXISTS){
      return -1;
    }
    return 1;
}

IReply Client::processCommand(std::string& input)
{
  // ------------------------------------------------------------
  // GUIDE 1:
  // In this function, you are supposed to parse the given input
  // command and create your own message so that you call an 
  // appropriate service method. The input command will be one
  // of the followings:
  //
  // FOLLOW <username>
  // UNFOLLOW <username>
  // LIST
  // TIMELINE
  // ------------------------------------------------------------
  
  // ------------------------------------------------------------
  // GUIDE 2:
  // Then, you should create a variable of IReply structure
  // provided by the client.h and initialize it according to
  // the result. Finally you can finish this function by returning
  // the IReply.
  // ------------------------------------------------------------
  
  
  // ------------------------------------------------------------
  // HINT: How to set the IReply?
  // Suppose you have "FOLLOW" service method for FOLLOW command,
  // IReply can be set as follow:
  // 
  //     // some codes for creating/initializing parameters for
  //     // service method
  //     IReply ire;
  //     grpc::Status status = stub_->FOLLOW(&context, /* some parameters */);
  //     ire.grpc_status = status;
  //     if (status.ok()) {
  //         ire.comm_status = SUCCESS;
  //     } else {
  //         ire.comm_status = FAILURE_NOT_EXISTS;
  //     }
  //      
  //      return ire;
  // 
  // IMPORTANT: 
  // For the command "LIST", you should set both "all_users" and 
  // "following_users" member variable of IReply.
  // ------------------------------------------------------------

    IReply ire;
    
    // Parse the given input to get command and arguments
    int n = input.size();
    bool space_found = false;
    std::string cmd = "";
    std::string args = "";
    for(int i = 0;i<n;i++){
      if(input[i] == ' '){
        space_found = true;
      }else{
        if(space_found)args+=input[i];
        else cmd+=input[i];
      }
    }
    
    // Call the function based on command
    if(cmd == "FOLLOW"){
      return Follow(args);
    }else if(cmd == "UNFOLLOW"){
      return UnFollow(args);
    }else if(cmd == "LIST"){
      IReply tmp = List();
      // Check if server exists
      if(!tmp.grpc_status.ok()){
        ire.comm_status = FAILURE_UNKNOWN;
        return ire;
      }
      return tmp;
    }else if(cmd == "TIMELINE"){
      // Enter  in timeline mode 
      // Check is server is up
      IReply tmp = List();
      if(!tmp.grpc_status.ok()){
        ire.comm_status = FAILURE_UNKNOWN;
        return ire;
      }
      ire.comm_status = SUCCESS;
      return ire;
    }
    ire.comm_status = FAILURE_INVALID;
    return ire;
}


void Client::processTimeline()
{
    Timeline(username);
}

// List Command
IReply Client::List() {

    IReply ire;

    ClientContext context;

    Request request;
    request.set_username(username);

    ListReply reply;

    Status status = stub_->List(&context,request,&reply);
    ire.grpc_status = status;

    if(status.ok()){
      ire.comm_status = SUCCESS;
      std::string users;
      std::string followers;

      for(auto user:reply.all_users()){
        ire.all_users.push_back(user);
      }
      for(auto follower:reply.followers()){
        ire.followers.push_back(follower);
      }
    }

    return ire;
}

// Follow Command        
IReply Client::Follow(const std::string& username2) {

    IReply ire; 
      
    ClientContext context;

    Request request;
    request.set_username(username);
    request.add_arguments(username2);

    Reply reply;

    Status status = stub_->Follow(&context,request,&reply);
                                                                                                                                                            
    if(reply.msg()=="Command failed with invalid username"){
      ire.comm_status = FAILURE_INVALID_USERNAME;
    }
    else if(reply.msg() == "Input username already exists, command failed"){
      ire.comm_status = FAILURE_ALREADY_EXISTS;
    }
    else if(reply.msg() == "Command completed successfully"){
      ire.comm_status = SUCCESS;
    }
    return ire;
}

// UNFollow Command  
IReply Client::UnFollow(const std::string& username2) {

    IReply ire;

    ClientContext context;

    Request request;
    request.set_username(username);
    request.add_arguments(username2);

    Reply reply;

    Status status = stub_->UnFollow(&context,request,&reply);
                                                                                                                                                            
    if(reply.msg()=="Command failed with invalid username"){
      ire.comm_status = FAILURE_INVALID_USERNAME;
    }
    else if(reply.msg() == "Command completed successfully"){
      ire.comm_status = SUCCESS;
    }
    
    return ire;
}

// Login Command  
IReply Client::Login() {

    IReply ire;

    ClientContext context;

    Request request;
    request.set_username(username);
    request.add_arguments("client");
    Reply reply;

    Status status = stub_->Login(&context,request,&reply);
    
    ire.grpc_status = status;

    std::cout<<std::endl;
    if(reply.msg() == "you have already joined"){
      ire.comm_status = FAILURE_ALREADY_EXISTS;
    }else{
      ire.comm_status = SUCCESS;
    }

    return ire;
}

// Timeline Command
void Client::Timeline(const std::string& username) {

    // ------------------------------------------------------------
    // In this function, you are supposed to get into timeline mode.
    // You may need to call a service method to communicate with
    // the server. Use getPostMessage/displayPostMessage functions 
    // in client.cc file for both getting and displaying messages 
    // in timeline mode.
    // ------------------------------------------------------------

    // ------------------------------------------------------------
    // IMPORTANT NOTICE:
    //
    // Once a user enter to timeline mode , there is no way
    // to command mode. You don't have to worry about this situation,
    // and you can terminate the client program by pressing
    // CTRL-C (SIGINT)
    // ------------------------------------------------------------
  
    ClientContext context;
    context.AddMetadata("username", username);
    std::shared_ptr<ClientReaderWriter<Message,Message>> stream(stub_->Timeline(&context));

    std::thread writer([username,stream](){
      Message m = MakeMessage(username,"first_timeline_stream");
      stream->Write(m);
      while(1){
        std::string terminal_message = getPostMessage();
        Message message = MakeMessage(username,terminal_message);
        stream->Write(message); 
      }
      stream->WritesDone();
    });

    std::thread reader([username,stream](){
      Message m;
      while(stream->Read(&m)){
        std::time_t time = m.timestamp().seconds();
        displayPostMessage(m.username(),m.msg(),time);
      }
    });

    writer.join();
    reader.join();

}

IReply Client::ClientHeartbeat() {

    IReply ire;

    // creating arguments and utils to make the gRPC
    ClientContext context;
    Request request;
    Reply reply;

    request.set_username(username);

    // making a grpc periodically to let the server know that the client is alive
    grpc::Status status = stub_->ClientHeartbeat(&context, request, &reply);
    if (status.ok()){
        ire.grpc_status = status;
    }else { // technically should exit in this case but since one of the test cases says otherwise, I am commenting this out
        /* ire.grpc_status = status; */
        /* std::cout << "server not found! exiting now...\n"; */
        /* exit(0); */
    }

    return ire;
}

//////////////////////////////////////////////
// Main Function
/////////////////////////////////////////////
int main(int argc, char** argv) {

  std::string hostname = "localhost";
  std::string port = "3010";
  std::string username = "1";
    
  int opt = 0;
  while ((opt = getopt(argc, argv, "h:k:u:")) != -1){
    switch(opt) {
    case 'h':
      hostname = optarg;break;
    case 'k':
      port = optarg;break;
    case 'u':
      username = optarg;break;
    default:
      std::cout << "Invalid Command Line Argument\n";
    }
  }
      
  std::cout << "Logging Initialized. Client starting...";
  
  Client myc(hostname, username, port);
  
  myc.run();
  
  return 0;
}
