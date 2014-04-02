/* This is written by Jinka Sai Sagar
 * 
 *
 *
 * */
#ifndef GRAPH
#define GRAPH

#include <libgen.h>

#include <iostream>
#include <string>

#include <boost/lexical_cast.hpp>

#include <mesos/resources.hpp>
#include <mesos/scheduler.hpp>


#include<stout/check.hpp>
#include<stout/flags.hpp>
#include<stout/os.hpp>
#include<stout/stringify.hpp>
#include </home/test/mesos-0.14.0/src/logging/flags.hpp>
#include <graphlab/options/graphlab_options.hpp>
#include <graphlab/scheduler/ischeduler.hpp>
#include <graphlab/scheduler/fifo_scheduler.hpp>
#include <graphlab/options/graphlab_options.hpp>
#include <graphlab/logger/logger.hpp>
#include <graphlab/graph/graph_basic_types.hpp>
using mesos::Scheduler;
using mesos::Resource;
using mesos::Offer;
using mesos::OfferID;
using mesos::SchedulerDriver;
using mesos::TaskStatus;
using mesos::MasterInfo;
using mesos::ExecutorID;
using mesos::FrameworkID;
using mesos::ExecutorInfo;
using mesos::FrameworkInfo;
using mesos::SlaveID;
using std::string;
using boost::lexical_cast;

#include "editresource.hpp";
using namespace mesos;
using mesos::Resources;
using namespace graphlab;
#include <vector>
#include <string>
#include <fstream>

#include <graphlab.hpp>
#include </home/test/GRAPHLABS/graphlab/src/graphlab/engine/warp_engine_edited.hpp>
//using namespace graphlab;


typedef distributed_graph<float , graphlab::empty> graph_type;

struct graphStatus {
int id;
std::string name;
};
template <class warp_engine_edited>
class pointer { 
   private:
	  warp_engine_edited* point;
	  ExecutorInfo executorInfo;
   public:
   
/* We are using the concept of inner class here. Pointer Class is outside class and GraphlabTestScheduler is inner one. Inner class implements the 
 * scheduler interface provided by mesos. This class connects the Graphlab framework to Mesos. Outer class brings the context of Warp engine to the Inner class
 * where we use it for stalling the computation of graphlab and resuming after it recieves a green signal from Mesos.  When Outer object is created
 * GraphlabTestScheduler's object is created
 */
 
	pointer(warp_engine_edited* ptr, const ExecutorInfo executor) : point(ptr),executorInfo(executor),myscheduler(*this,executorInfo)
	{
		/*ExecutorInfo executorInfo;
         	executorInfo.mutable_executor_id()->set_value("default");
        	executorInfo.mutable_command()->set_value("~/GRAPHLABS/graphlab/src/graphlab/engine/executor.hpp");
            	executorInfo.set_name("Graphlab Executor");
            	executorInfo.set_source("graphlab.cpp");
//		myscheduler(*this,executorInfo);*/
	}

class GraphlabTestScheduler : public Scheduler
{
public:
	pointer &self;
	/* Giving Static Access to Outer Access */
	GraphlabTestScheduler(pointer &m1, const ExecutorInfo& exec) : self(m1),executor(exec) {} //Edit
	virtual ~GraphlabTestScheduler(){}
	size_t memory;
	size_t ncpus;
	SchedulerDriver* drivered;
	vector<Offer> offered;
	resourceCalculation resourced;
//	warp_engine_type *edit;
//typedef warp_engine_edited* pointer;
	void setOffer(const vector<Offer>& offer)
	{
		 offered = offer;
		 logstream(LOG_INFO) << "Size of offer in set offer"<<offered.size()<<std::endl;
	}
	vector<Offer> getOffer()
	{
		return offered;
	}

  virtual void registered(SchedulerDriver*,
                          const FrameworkID&,
                          const MasterInfo&)
  {
   std::cout << "Graphlab Registered yayy!" << std::endl;
  }

   virtual void reregistered(SchedulerDriver*, const MasterInfo& masterInfo) {
	std::cout << "Graphlab ReRegistered!" << std::endl;
}

   virtual void disconnected(SchedulerDriver* driver) {
	logstream(LOG_INFO) << "SchedulerDriver is disconnected"<<std::endl;
  }

 virtual void resourceOffers(SchedulerDriver* driver,
                              const vector<Offer>& offers)
  {
	warp_engine_edited* trail = self.point;
	std::cout << "Graphlab Resource offered!" << std::endl;
	logstream(LOG_INFO) << "In resource Offers function " << std::endl;
//	resourced.resource(driver,offers, point);
	//resourceCalculation resourced;
	resourced.resource(driver,offers,trail,executor);
  }
   virtual void offerRescinded(SchedulerDriver* driver,
                              const OfferID& offerId) {
    logstream(LOG_INFO) << "Offer Rescinded " << std::endl;
}

  virtual void statusUpdate(SchedulerDriver* driver, const TaskStatus& status)
  {
    switch(status.state()){
    case TASK_FINISHED:
	logstream(LOG_INFO) << " Task is finished" << std::endl;
        driver->stop();
    case TASK_FAILED:
	logstream(LOG_INFO) << "Task is failed: "<<std::endl;
        driver->abort();
    case TASK_KILLED:
	logstream(LOG_INFO) << " Task is killed " <<std::endl;
        driver->stop();
    case TASK_LOST:
	logstream(LOG_INFO) << " Task is lost " <<std::endl;
        //driver->stop();
        driver->abort();
    }
  }

  virtual void frameworkMessage(SchedulerDriver* driver,
                                const ExecutorID& executorId,
                                const SlaveID& slaveId,
                                const string& data) {
    logstream(LOG_INFO)<<"Framework message of "<<sizeof(data)<<" bytes from executor "<<std::endl;
  }

  virtual void slaveLost(SchedulerDriver* driver, const SlaveID& sid) {
     logstream(LOG_INFO) << "Lost Slave: " <<std::endl;
  }

 virtual void executorLost(SchedulerDriver* driver,
                            const ExecutorID& executorID,
                            const SlaveID& slaveID,
                          int status) {
     logstream(LOG_ERROR)<<"Executor is lost"<<std::endl;
}

  virtual void error(SchedulerDriver* driver, const string& message)
  {
    logstream(LOG_ERROR)<<"Error is "<<message<<std::endl;
  }
private:
  const ExecutorInfo executor;
  std::string role;
  int tasksLaunched;
  int tasksFinished;
  int totalTasks;
}myscheduler;
//};
};
#endif

