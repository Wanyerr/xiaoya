#include <iostream>
#include "mprpcapplication.h"
#include "user.pb.h"
#include "mprpcchannel.h"

int main(int argc,char **argv)
{
    MprpcApplication::Init(argc,argv);
    
    fixbug::UserServiceRpc_Stub stub(new MprpcChannel());
    fixbug::LoginRequest request;
    request.set_name("zhang san");
    request.set_pwd("123456");
    
    fixbug::LoginResponse response;

    stub.Login(nullptr,&request,&response,nullptr);
    if(response.result().errcode()==0)
    {
        std::cout<<"rpc login response:"<<response.success()<<std::endl;
    }
    else
    {
        std::cout<<"rpc login response error:"<<response.result().errmsg()<<std::endl;

    }

    
    return 0;
}
