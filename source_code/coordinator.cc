#include <algorithm>
#include <cstdio>
#include <ctime>

#include <google/protobuf/timestamp.pb.h>
#include <google/protobuf/duration.pb.h>
#include <chrono>
#include <sys/stat.h>
#include <sys/types.h>
#include <utility>
#include <vector>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <mutex>
#include <stdlib.h>
#include <unistd.h>
#include <google/protobuf/util/time_util.h>
#include <grpc++/grpc++.h>
#include<glog/logging.h>
#define log(severity, msg) LOG(severity) << msg; google::FlushLogFiles(google::severity); 
#include "coordinator.grpc.pb.h"
#include "coordinator.pb.h"
#include <string>
using google::protobuf::Timestamp;
using google::protobuf::Duration;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReader;
using grpc::ServerReaderWriter;
using grpc::ServerWriter;
using grpc::Status;
using csce662::CoordService;
using csce662::ServerInfo;
using csce662::Confirmation;
using csce662::ID;
using csce662::ServerList;
using csce662::SynchService;
using csce662::PathAndData;
using csce662::Path;

using grpc::ClientContext;

struct zNode{
    int serverID;
    std::string hostname;
    std::string port;
    std::string type;
    std::time_t last_heartbeat;
    bool missed_heartbeat;
    bool isActive();
    bool isMaster;

};

//potentially thread safe 
std::mutex v_mutex;
std::vector<zNode*> cluster1;
std::vector<zNode*> cluster2;
std::vector<zNode*> cluster3;

// creating a vector of vectors containing znodes
std::vector<std::vector<zNode*>> clusters = {cluster1, cluster2, cluster3};


//func declarations
int findServer(std::vector<zNode*> v, int id); 
std::time_t getTimeNow();
void checkHeartbeat();


bool zNode::isActive(){
    bool status = false;
    if(!missed_heartbeat){
        status = true;
    }else if(difftime(getTimeNow(),last_heartbeat) < 10){
        status = true;
    }
    return status;
}


class CoordServiceImpl final : public CoordService::Service {

    Status GetSlave(ServerContext* context,const ID* request,ServerInfo* response ) override{
        int id = request->id();
        std::cout<<"Coming here"<<std::endl;
        zNode* znode = clusters[id-1][1];
        
        std::cout<<"Coming here"<<std::endl;

        response->set_serverid(znode->serverID);
        response->set_hostname(znode->hostname);
        response->set_port(znode->port);
        response->set_type("slave");
        response->set_clusterid(id);
        response->set_ismaster(false);

        return Status::OK;
    }

    Status Heartbeat(ServerContext* context, const ServerInfo* serverinfo, Confirmation* confirmation) override {
        // Your code here

        // Set last heartbeat from the server equal to current time
        
        int server_id = serverinfo->serverid();
        int cluster_id = serverinfo->clusterid();
        std::string hostname = serverinfo->hostname();
        std::string port = serverinfo->port();
        std::string type = serverinfo->type();
        
        if(serverinfo->type() == "synchronizer"){
        //Register the synchronizer
    
        zNode* znode = new zNode();
        znode->serverID = server_id;
        znode->port = port;
        znode->type = type;
        znode->hostname = hostname;
        
        //First synchronizer in that cluster is the master
        if((server_id-1)/3 == 0){
            znode->isMaster = true;
        }else{
            znode->isMaster = false;
        }
        //Zero-based indexing
        std::vector<zNode*> c = clusters[cluster_id-1];
        c.push_back(znode);
        clusters[cluster_id-1] = c;
        
        }else{
            log(INFO,"Received Heartbeat from Server " + std::to_string(server_id) + ", Cluster " + std::to_string(cluster_id));

            zNode* node = clusters[cluster_id-1][server_id-1];
            node->last_heartbeat = getTimeNow();

            confirmation->set_status(node->isActive());
        }
        
        return Status::OK;
    }

    //function returns the server information for requested client id
    //this function assumes there are always 3 clusters and has math
    //hardcoded to represent this.
    Status GetServer(ServerContext* context, const ID* id, ServerInfo* serverinfo) override {
        // The coordinator routes the client to the appropriate server
        
        int cluster_id = (id->id()-1)%3+1;
        int server_id = 1;
        zNode* node = clusters[cluster_id-1][server_id-1]; 


        //Assign slave if master is not active
        if(!node->isActive()){
            node = clusters[cluster_id-1][1];
        }
        
        log(INFO,"Received Request from Client " + std::to_string(id->id()));
 

        serverinfo->set_serverid(node->serverID);
        serverinfo->set_hostname(node->hostname);
        serverinfo->set_port(node->port);       
        serverinfo->set_type(node->type); 
               return Status::OK;
    }

    Status GetAllFollowerServers(ServerContext* context, const ID* id, ServerList* serverlist) override {
    
    log(INFO, "Got GetAllFollowerServers for syncId: " + std::to_string(id->id()));
    // Check for cluster 1
    for(int i = 0;i<clusters[0].size();i++){
        zNode* node = clusters[0][i];
        if(node->type == "synchronizer"){
            
            serverlist->add_serverid(node->serverID);
            serverlist->add_hostname(node->hostname);
            serverlist->add_port(node->port);
            serverlist->add_type("follower");

        }
    }

    // Check for cluster 2
    for(int i = 0;i<clusters[1].size();i++){
        zNode* node = clusters[1][i];
        if(node->type == "synchronizer"){
            
            serverlist->add_serverid(node->serverID);
            serverlist->add_hostname(node->hostname);
            serverlist->add_port(node->port);
            serverlist->add_type("follower");

        }
    }

    // Check for cluster 3
    for(int i = 0;i<clusters[2].size();i++){

            zNode* node = clusters[2][i];
        if(node->type == "synchronizer"){
            serverlist->add_serverid(node->serverID);
            serverlist->add_hostname(node->hostname);
            serverlist->add_port(node->port);
            serverlist->add_type("follower");

        }
    }
    

        return Status::OK;
    }

    Status create(ServerContext* context, const PathAndData* request, csce662::Status* status){
        // Receives information from the server
        std::string data = request->data();

        int server_id;
        int cluster_id;

        std::string temp = "";
        for(auto s:data){
            if(s!=','){
                temp += s;
            }else{
                server_id = stoi(temp);
                temp = "";
            }
        }
        cluster_id = stoi(temp);
        
        // Check if serveral already exists, then restart
        for(auto &s:clusters[cluster_id-1]){
            if(s->serverID == server_id){
                if(s->missed_heartbeat == true){
                    std::cout << "Restarting server " << s->serverID << std::endl;
                    s->missed_heartbeat = false;
                    s->last_heartbeat = getTimeNow();
                }
                return Status::OK;
            }
        }
        std::string hostname = "";
        std::string port = "";
        temp = "";

        std::string path = request->path();
        for(auto s:path){
            if(s!=':'){
                temp += s;
            }else{
                hostname = temp;
                temp = "";
            }
        }
        port = temp;

        log(INFO,"Received Request from Server " + std::to_string(server_id) + ", Cluster " + std::to_string(cluster_id));

        
        // Create a new Zookeeper node
        zNode* znode = new zNode();
        znode->serverID = server_id;
        znode->hostname = hostname;
        znode->port = port;

        //Check if master or a slave
        if(clusters[cluster_id-1].size() == 0){
            znode->type = "master";
        }else{
            znode->type = "slave";
        }
        if(cluster_id == 1){
            std::vector<zNode*> c = clusters[0];
            c.push_back(znode);
            clusters[0] = c;
        }else if(cluster_id == 2){
            std::vector<zNode*> c = clusters[1];
            c.push_back(znode);
            clusters[1] = c;
        }else{
            std::vector<zNode*> c = clusters[2];
            c.push_back(znode);
            clusters[2] = c;

        }

        // Print server ID , hostname and port
        std::cout<<znode->serverID<<" "<<znode->hostname<<" "<<znode->port<<std::endl;
        
        //Create directory
        std::string directory_name = "cluster"+std::to_string(cluster_id);
        std::filesystem::create_directory(directory_name);

        std::filesystem::create_directory(directory_name+"/"+std::to_string(server_id));
        log(INFO, "Server Directory Created");
        return Status::OK;
    }

    

};

void RunServer(std::string port_no){
    //start thread to check heartbeats
    std::thread hb(checkHeartbeat);
    //localhost = 127.0.0.1
    std::string server_address("127.0.0.1:"+port_no);
    CoordServiceImpl service;
    //grpc::EnableDefaultHealthCheckService(true);
    //grpc::reflection::InitProtoReflectionServerBuilderPlugin();
    ServerBuilder builder;
    // Listen on the given address without any authentication mechanism.
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    // Register "service" as the instance through which we'll communicate with
    // clients. In this case it corresponds to an *synchronous* service.
    builder.RegisterService(&service);
    // Finally assemble the server.
    std::unique_ptr<Server> server(builder.BuildAndStart());
    std::cout << "Server listening on " << server_address << std::endl;

    // Wait for the server to shutdown. Note that some other thread must be
    // responsible for shutting down the server for this call to ever return.
    server->Wait();
}

int main(int argc, char** argv) {

    std::string port = "3010";
    int opt = 0;
    while ((opt = getopt(argc, argv, "p:")) != -1){
        switch(opt) {
            case 'p':
                port = optarg;
                break;
            default:
                std::cerr << "Invalid Command Line Argument\n";
        }
    }

    std::string log_file_name = std::string("coordinator-") + port;
    google::InitGoogleLogging(log_file_name.c_str());

    RunServer(port);
    return 0;
}



void checkHeartbeat(){
    while(true){
        //check servers for heartbeat > 10
        //if true turn missed heartbeat = true
        // Your code below

        v_mutex.lock();

        // iterating through the clusters vector of vectors of znodes
        for (auto& c : clusters){
            for(auto& s : c){
                if(difftime(getTimeNow(),s->last_heartbeat)>10){
                    if(s->type == "synchronizer")continue;
                    std::cout << "missed heartbeat from server " << s->serverID << std::endl;
                    if(!s->missed_heartbeat){
                        s->missed_heartbeat = true;
                        s->last_heartbeat = getTimeNow();
                    }
                }
            }
        }

        v_mutex.unlock();

        sleep(3);
    }
}


std::time_t getTimeNow(){
    return std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
}

