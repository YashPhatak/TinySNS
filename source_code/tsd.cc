/*
 *
 * Copyright 2015, Google Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 *
 *     * Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above
 * copyright notice, this list of conditions and the following disclaimer
 * in the documentation and/or other materials provided with the
 * distribution.
 *     * Neither the name of Google Inc. nor the names of its
 * contributors may be used to endorse or promote products derived from
 * this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 */

#include <ctime>

#include <google/protobuf/timestamp.pb.h>
#include <google/protobuf/duration.pb.h>
#include<filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <stdlib.h>
#include <unistd.h>
#include <google/protobuf/util/time_util.h>
#include <grpc++/grpc++.h>
#include<glog/logging.h>
#define log(severity, msg) LOG(severity) << msg; google::FlushLogFiles(google::severity); 

#include "sns.grpc.pb.h"

#include "coordinator.pb.h"
#include "coordinator.grpc.pb.h"
#include<thread>
using google::protobuf::Timestamp;
using google::protobuf::Duration;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReader;
using grpc::ServerReaderWriter;
using grpc::ServerWriter;
using grpc::Status;
using csce662::Message;
using csce662::ListReply;
using csce662::Request;
using csce662::Reply;
using csce662::SNSService;
using csce662::CoordService;
using csce662::PathAndData;
using grpc::ClientContext;
using csce662::ServerInfo;
using csce662::Confirmation;
using csce662::Path;
using csce662::ID;


std::time_t getTimeNow(){
    return std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
}
bool slave_turned_master = false;
bool latest_20_to_show = true;
struct Client {
  std::string username;
  bool first_request = true;
  bool connected = true;
  bool first_slave_request = true;
  int following_file_size = 0;
  std::time_t last_heartbeat = getTimeNow();
  bool missed_heartbeat = false;
  std::vector<Client*> client_followers;
  std::vector<Client*> client_following;
  ServerReaderWriter<Message, Message>* stream = 0;
  bool operator==(const Client& c1) const{
    return (username == c1.username);
  }
};

//Vector that stores every client that has been created
std::vector<Client*> client_db;
std::unique_ptr<CoordService::Stub> stub_;

std::string directory_name = "";
std::string serverID;
std::string clusterID;


void checkHeartbeat();

// Heartbeat between client and Server
class SNSServiceImpl final : public SNSService::Service {

  Status ClientHeartbeat(ServerContext* context, const Request* request, Reply* reply) override {

        std::string username = request->username();

        std::cout << "got a heartbeat from client: " << username << std::endl; 
        Client* c = getClient(username);
        if (c != NULL){
            c->last_heartbeat = getTimeNow();

        } else {
            std::cout << "client was not found, for some reason!\n";
            return Status::CANCELLED;
        }

        return Status::OK;
    }
  
    
  Status List(ServerContext* context, const Request* request, ListReply* list_reply) override {
    

    std::string line;
    // Read the posts from the latest file

    std::ifstream users_master("cluster"+clusterID+"/"+ std::to_string(1) + "/all_users.txt");
    std::ifstream users_slave("cluster"+clusterID+"/"+ std::to_string(2) + "/all_users.txt");

    std::vector<std::string>u_master;
    while(getline(users_master,line)){
      u_master.push_back(line);
    }

    std::vector<std::string>u_slave;
    while(getline(users_slave,line)){
      u_slave.push_back(line);
    }
    
    std::vector<std::string>users_list;
    if(u_master.size()>=u_slave.size()){
      users_list = u_master;
    }else{
      users_list = u_slave;
    }

    std::sort(users_list.begin(),users_list.end());
    for(auto u:users_list){
      list_reply->add_all_users(u);
    }
    

    std::string username = request->username();
    Client* client = NULL;


    std::ifstream input_master("cluster"+clusterID+"/"+ std::to_string(1) + "/"+ username + "_followers.txt");
    std::ifstream input_slave("cluster"+clusterID+"/"+ std::to_string(2) + "/"+ username + "_followers.txt");
   
    

    std::vector<std::string>f_master;
    while(getline(input_master,line)){
      f_master.push_back(line);
    }

    std::vector<std::string>f_slave;
    while(getline(input_slave,line)){
      f_slave.push_back(line);
    }
    std::vector<std::string>followers;
    if(f_master.size()>=f_slave.size()){
      followers = f_master;
    }else{
      followers = f_slave;
    }

    std::sort(followers.begin(),followers.end());
    for(auto follower:followers){
      list_reply->add_followers(follower);
    }

    users_master.close();
    users_slave.close();
    input_master.close();
    input_slave.close();
    return Status::OK;
  }

  Status Follow(ServerContext* context, const Request* request, Reply* reply) override {

    for(auto c:client_db){
      std::cout<<c->username<<std::endl;
    }
    std::string username = request->username();
    std::string username2 = request->arguments(0);

    int user = std::stoi(username2);
    bool same_cluster = true;
    if((user-1)%3 + 1 != std::stoi(clusterID)){
      same_cluster = false;
    }
    std::cout<<same_cluster<<std::endl;

    // Case when the user tries to follow itself
    if(username == username2){
        reply->set_msg("Input username already exists, command failed");
  
    }
    else{
    Client* c1 = NULL,*c2 = NULL;
    for(auto client:client_db){
      if(client->username == username){
        c1 = client;
      }
      else if(client->username == username2){
        c2 = client;
      }
    }
    if(c1&&c2)
    std::cout<<c1->username<<" "<<c2->username<<std::endl;
    if(same_cluster && (c1 == NULL||c2 == NULL)){
      //Check if user exists
      reply->set_msg("Command failed with invalid username");
      
    }else{
      //Check if user already following
      bool found = false;
      std::cout<<"Checking if already following"<<std::endl;
      if(same_cluster){
      for(auto client:c1->client_following){
        std::cout<<"Coming to check"<<std::endl;
        if(client->username == username2){
          found = true;
          break;
        }
      }
      }
      if(found){
        reply->set_msg("Input username already exists, command failed");
      }else{
        std::cout<<"Coming inside"<<std::endl;
        if(same_cluster)
        c1->client_following.push_back(c2);
        if(same_cluster)
        c2->client_followers.push_back(c1);
        // Create the user files and store the follower information
        
        if(same_cluster){           
        std::ofstream follower_file(directory_name + "/" + username2 + "_followers.txt",std::ios::app|std::ios::out|std::ios::in);
        follower_file<<username+"\n";     
        } 
        
        std::cout<<"Files 1 Created"<<std::endl;  
        std::ofstream following_file(directory_name + "/" + username + "_following.txt",std::ios::app|std::ios::out|std::ios::in);
        following_file<<username2+"\n";
        
        std::cout<<"Files 2 Created"<<std::endl; 

        // Replication logic for slave server  
        if(serverID == "1"){
        ClientContext context;
        ID id;
        id.set_id(stoi(clusterID));
        ServerInfo serverinfo;

        stub_->GetSlave(&context,id,&serverinfo);

        std::string login_info = serverinfo.hostname() + ":" + serverinfo.port();
        std::cout<<serverinfo.hostname()<<" "<<serverinfo.port()<<std::endl;
        // Create a stub
        auto slave_stub_ = SNSService::NewStub(grpc::CreateChannel(login_info, grpc::InsecureChannelCredentials()));

        ClientContext context2;

        Request request2;
        request2.set_username(username);
        request2.add_arguments(username2);

        Reply reply2;
        slave_stub_->Follow(&context2,request2,&reply2);
        }
        reply->set_msg("Command completed successfully");
      }
    }
    }
    std::cout<<"Follow completed"<<std::endl;
    return Status::OK; 
  }

  Status UnFollow(ServerContext* context, const Request* request, Reply* reply) override {

    std::string username = request->username();
    std::string username2 = request->arguments(0);

    // Case when the user tries to unfollow itself
    if(username == username2){
        reply->set_msg("Command failed with invalid username");
    }
    else{
    // Get the client pointer for username from client_db
    Client* c1 = NULL,*c2 = NULL;
    for(auto client:client_db){
      if(client->username == username){
        c1 = client;
      }
      else if(client->username == username2){
        c2 = client;
      }
    }
    if(c1 == NULL||c2 == NULL){
      //Check if user exists
      reply->set_msg("Command failed with invalid username");
      
    }else{
      //Check if user already following
      bool found = false;
      for(auto client:c1->client_following){
        if(client->username == c2->username){
          found = true;
          break;
        }
      }
      if(!found){
        reply->set_msg("Command failed with invalid username");
      }else{
        // Remove user2 from user1's following list 
        c1->client_following.erase(std::find(c1->client_following.begin(),c1->client_following.end(),c2));
        // Remove user1 from user2's follower list 
        c2->client_followers.erase(std::find(c2->client_followers.begin(),c2->client_followers.end(),c1));
        reply->set_msg("Command completed successfully");
      }
    }
    }
    return Status::OK;
  }

  // RPC Login
  Status Login(ServerContext* context, const Request* request, Reply* reply) override {

    Client* client = new Client();
    std::string username = request->username();
    
    std::string type = request->arguments(0);
    Client* client_ = NULL;

    // Replication logic for slave server
    if(serverID == "2" && type != "master"){
      for(auto client:client_db){
        if(client->username == username){
          client_ = client; 
        }
      }
      
      if(client_ && client_->first_slave_request){
        client_->connected = false;
        client_->first_slave_request = false;
        
      }
      slave_turned_master = true;
    } 

    bool new_user = true;
    //check if username exists in client_db
    for(auto client:client_db){
      if(client->username == username){

          new_user = false;
          if(client->connected){
          reply->set_msg("you have already joined");
          log(ERROR,"user has already joined");
          }
          else{
           
          reply->set_msg("you are logged in");
          client->connected = true;
          client->missed_heartbeat = false;
          
          }
          return Status::OK;
      }


    }
    client->username = username;
    client->connected = true;

    client_db.push_back(client);
    
    reply->set_msg("you are logged in");

    // If the client is logging in for the first time, store the information in all_users.txt file.
    if(client->first_request){
            std::string usersFile = "./cluster" + clusterID + "/" + serverID + "/all_users.txt";
            std::ofstream allusers_file(usersFile,std::ios::app | std::ios::out | std::ios::in); 
            allusers_file<<username<<std::endl; 
            allusers_file.close();
            client->first_request = false; 
    } 


    // Create the user files
    std::ofstream follower_file(directory_name + "/" + username + "_followers.txt",std::ios::app|std::ios::out|std::ios::in);

    std::ofstream following_file(directory_name + "/" + username + "_following.txt",std::ios::app|std::ios::out|std::ios::in);
    
    // Replication logic for slave server
    if(serverID == "1"){
    ClientContext context;
    ID id;
    id.set_id(stoi(clusterID));
    ServerInfo serverinfo;

    stub_->GetSlave(&context,id,&serverinfo);

    std::string login_info = serverinfo.hostname() + ":" + serverinfo.port();
    std::cout<<serverinfo.hostname()<<" "<<serverinfo.port()<<std::endl;
    // Create a stub
    auto slave_stub_ = SNSService::NewStub(grpc::CreateChannel(login_info, grpc::InsecureChannelCredentials()));

    ClientContext context2;

    Request request2;
    request2.set_username(username);
    request2.add_arguments("master");

    Reply reply2;
    slave_stub_->Login(&context2,request2,&reply2);
    }
    return Status::OK;
  }
  
  Client* getClient(std::string username){
    for(auto c:client_db){
      if(c->username==username){
        return c;
      }
    }
    return NULL;
  }

  // The function reads the stream files for the users and puts the message into their streams.
  static void GetLatestPostsFromFile(ServerReaderWriter<Message, Message>* stream,std::string username){
   
    std::ofstream fs(directory_name + "/" + username + "_stream.txt", std::ios::out | std::ios::trunc);
    fs.close();
    while(true){
    std::cout<<"Thread Running "<<stream<<std::endl;  
    
    
    
    if(!stream)return;
    std::string filePath =  directory_name + "/" + username + "_stream.txt";

    std::ifstream file(filePath);
    std::string line;

    while (true) {
        // Read the first line
        std::getline(file, line);
        if (file.eof()) break; // Exit loop if end of file
        
        if (line.substr(0, 1) != "T") {
            sleep(10);
            continue;
        }
        std::stringstream ss(line.substr(2)); 
        std::string ts;
        ss >> ts;

        // Read the second line
        std::getline(file, line);
        
        std::stringstream userStream(line.substr(2)); 
        std::string user;
        userStream >> user;

        // Read the third line
        std::getline(file, line);
        
        std::string post = line.substr(2); 
        Message new_msg;
        new_msg.set_msg(post + "\n");
        new_msg.set_username(user);
        google::protobuf::Timestamp* timestamp = new google::protobuf::Timestamp();

        int64_t seconds = std::stoll(ts);
        timestamp->set_seconds(seconds);
        timestamp->set_nanos(0);

        new_msg.set_allocated_timestamp(timestamp);
        
        stream->Write(new_msg);
        std::cout<<"Coming here after call stream"<<std::endl;
        std::getline(file, line); // This will discard the extra line
    }

    file.close();

    // Delete contents of the file once streamed
    std::ofstream fileStream(filePath, std::ios::out | std::ios::trunc);
    fileStream.close();

    sleep(10);
    }
    
  }
  Status Timeline(ServerContext* context, 
		ServerReaderWriter<Message, Message>* stream) override {

    Message m;
    int index;
    // Create a seperate thread for streaming for different clusters
    auto metadata_map_ = context->client_metadata();
    auto it_ = metadata_map_.find("username");
    std::string client_username(it_->second.data(), it_->second.size());
    

     auto metadata_map = context->client_metadata();
     std::string posts = client_username;
    if(serverID == "2" && !(metadata_map.size() == 1|| metadata_map.size() == 2) && !slave_turned_master){
      int id;
      auto it = metadata_map.find("username");
      std::string u(it->second.data(), it->second.size());
      it = metadata_map.find("post");
      
      std::string content(it->second.data(), it->second.size());
      std::replace(content.begin(), content.end(), '|', '\n');
      content = content + "\n\n";
      std::ofstream file(directory_name + "/"+u+"_posts.txt",std::ios::app|std::ios::out|std::ios::in);
      file<<content;
      Client* c = getClient(u);
      for(int i = 0;i<client_db.size();i++){
        if(client_db[i] == c){
          id = i;
          break;
        }
      }
      Client* client = client_db[id];
      for(Client* f: client->client_followers){
          std::ofstream writer(directory_name + "/" + f->username+"_following_posts.txt",std::ios::app|std::ios::out|std::ios::in);
          writer<<content;
      }
      return Status::OK;
    }

    // Create a new thread for stream updations
    std::thread check_latest_posts(GetLatestPostsFromFile,stream, client_username); 
    // check_latest_posts.detach();
    Client* c;
    while(stream->Read(&m)){
      

      std::string username = m.username();
      c = getClient(username);

      for(int i = 0;i<client_db.size();i++){
        if(client_db[i] == c){
          index = i;
          break;
        }
      }
      c->stream = stream;


      std::string filename = username + "_posts.txt";
      
      // Create the user file
      std::ofstream user_file(directory_name + "/" + filename,std::ios::app|std::ios::out|std::ios::in);
      google::protobuf::Timestamp timestamp = m.timestamp();
      int64_t time = timestamp.seconds();

      // format file output
      std::string file_input = "T " + std::to_string(time) + "\nU " + m.username() + "\nW " + m.msg() + "\n"; 
      

      if(m.msg() != "first_timeline_stream"){
        user_file << file_input;
        user_file.close();

      } 

      else{
        std::string line;
        std::vector<std::string> newest_twenty;

        
        std::ifstream in(directory_name + "/" + username + "_following_posts.txt");
        int cnt = 0;

        bool exists = client_username == "5";

        while(getline(in,line)){
          newest_twenty.push_back(line);
        }
        Message new_msg;
        
        // Retrieve latest 20 posts
        int n = newest_twenty.size();
        
        for(int i=0;i<n&&cnt<20&&!exists;i+=4){
              new_msg.set_msg(newest_twenty[i+2].substr(2) + '\n');
              new_msg.set_username(newest_twenty[i+1].substr(2));
      
              google::protobuf::Timestamp* timestamp = new google::protobuf::Timestamp();

              int64_t seconds = std::stoll(newest_twenty[i].substr(2));
              timestamp->set_seconds(seconds);
        
              timestamp->set_nanos(0);
              new_msg.set_allocated_timestamp(timestamp);

              stream->Write(new_msg);
            cnt++;
        }
        
        
        continue;
      }

    
      Client* client = client_db[index];
      for(Client* f: client->client_followers){
            if(client->stream != 0){
            Message new_msg;
            new_msg.set_msg(m.msg());
            new_msg.set_username(m.username());
            Timestamp* timestamp = new google::protobuf::Timestamp();
            timestamp->set_seconds(m.timestamp().seconds());


            new_msg.set_allocated_timestamp(timestamp);

            // When user posts add that message in the stream of its followers if they are in timeline mode
            if(f->stream != 0){
            f->stream->Write(new_msg);
            }

            // When user posts add that message in the following file of its followers

            std::ofstream writer(directory_name + "/" + f->username+"_following_posts.txt",std::ios::app|std::ios::out|std::ios::in);
            writer<<file_input;
        
            writer.close();
            }
      }
      // Replicate logic for slave server 
      if(serverID == "1"){
        ClientContext context;
        ID id;
        id.set_id(stoi(clusterID));
        ServerInfo serverinfo;

        stub_->GetSlave(&context,id,&serverinfo);

        std::string login_info = serverinfo.hostname() + ":" + serverinfo.port();
        std::cout<<serverinfo.hostname()<<" "<<serverinfo.port()<<std::endl;
        // Create a stub
        auto slave_stub_ = SNSService::NewStub(grpc::CreateChannel(login_info, grpc::InsecureChannelCredentials()));

        ClientContext context2;
        context2.AddMetadata("username", username);
        // format file output
        std::string f_username = m.username();
        std::string f_msg = m.msg();
        f_msg.pop_back();

        std::cout<<f_username<<std::endl;
        std::cout<<f_msg<<std::endl;
        std::string file_contents = "T " + std::to_string(time)+ "|U " + f_username + "|W " + f_msg; 
      

        context2.AddMetadata("post", file_contents);
        slave_stub_->Timeline(&context2);

      }
    }
    check_latest_posts.join();
    return Status::OK;

  }

};

void sendHeartbeat(int server_id,int cluster_id,std::string hostname,std::string port){
  while(true){
    // Send heartbeat to the coordinator every 5 seconds
    ClientContext context;
    Confirmation confirmation;

    ServerInfo serverInfo;
    serverInfo.set_serverid(server_id);
    serverInfo.set_clusterid(cluster_id);
    serverInfo.set_hostname(hostname);
    serverInfo.set_port(port);
    if(server_id == 1){
      serverInfo.set_type("master");
    }else{
      serverInfo.set_type("slave");
    }

    log(INFO, "Sending Heartbeat to coordinator");
    grpc::Status status = stub_->Heartbeat(&context,serverInfo,&confirmation);
    
    sleep(5);
  }
}

grpc::Status connectToCoordinator(std::string login_info,std::string server_address,std::string server_id,std::string cluster_id){
  // Create a stub
  stub_ = CoordService::NewStub(grpc::CreateChannel(login_info, grpc::InsecureChannelCredentials()));

  PathAndData pathAndData;
  pathAndData.set_path(server_address);
  pathAndData.set_data(server_id + ","+cluster_id);
  Path path;
  path.set_path(server_address);
  
  ClientContext context;
  csce662::Status status;
  // Connect to coordinator

  log(INFO, "Connecting to coordinator");
  return stub_->create(&context,pathAndData,&status);
}

void RunServer(std::string cluster_id,std::string server_id,std::string coordinator_ip,std::string coordinator_port,std::string port_no){

  std::string server_address = "0.0.0.0:"+port_no;

  std::string login_info = coordinator_ip + ":" + coordinator_port;


  std::thread hb_client(checkHeartbeat);


  // Register server with coordinator
  grpc::Status connect = connectToCoordinator(login_info,server_address,server_id,cluster_id);
  
  // Send heartbeat to coordinator in a seperate thread
  std::thread hb(sendHeartbeat,stoi(server_id),stoi(cluster_id),"0.0.0.0",port_no);
  
  SNSServiceImpl service;

  ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  std::unique_ptr<Server> server(builder.BuildAndStart());
  std::cout << "Server listening on " << server_address << std::endl;
  log(INFO, "Server listening on "+server_address);

  server->Wait();
}

void checkHeartbeat(){
    if(serverID == "1" || (serverID == "2" && slave_turned_master)){
    while(true){
        

        for (auto client : client_db){
            if(difftime(getTimeNow(),client->last_heartbeat) > 20){
                std::cout << "missed heartbeat from client with id " << client->username << std::endl;
                if(!client->missed_heartbeat){
                
                        std::cout << "setting the client's values in the DB to show that it is down!\n";
                        client->connected = false;
                        // client->stream = nullptr;
                        client->missed_heartbeat = true;
                        client->last_heartbeat = getTimeNow();
                  } else{
                        std::cout << "SUDDENLY, THE CLIENT CANNOT BE FOUND?!\n";
                    }
                
            }
        }

        sleep(5);
    }
    }
}


int main(int argc, char** argv) {

  std::string port = "3010";
  std::string cluster_id = "1";
  std::string server_id = "1";
  std::string coordinator_ip = "127.0.0.1";
  std::string coordinator_port = "9090";
  
  int opt = 0;
  while ((opt = getopt(argc, argv, "c:s:h:k:p:")) != -1){
    switch(opt) {
      
      case 'c':
          cluster_id = optarg;break;
      case 's':
          server_id = optarg;break;
      case 'h':
          coordinator_ip = optarg;break;
      case 'k':
          coordinator_port = optarg;break;
      case 'p':
          port = optarg;break;
      default:
	  std::cerr << "Invalid Command Line Argument\n";
    }
  }
  
  std::string log_file_name = std::string("server-") + port;
  google::InitGoogleLogging(log_file_name.c_str());
  log(INFO, "Logging Initialized. Server starting...");


  directory_name = "cluster"+cluster_id+"/"+server_id;
  serverID = server_id;
  clusterID = cluster_id;
  RunServer(cluster_id,server_id,coordinator_ip,coordinator_port,port);

  return 0;
}
